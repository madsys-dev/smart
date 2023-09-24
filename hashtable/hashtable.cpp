/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2022-2023 Feng Ren, Tsinghua University 
 * 
 * Implementation is based on the description of the following
 * paper:
 * 
 * Pengfei Zuo, et. al. One-sided RDMA-Conscious Extendible Hashing for Disaggregated Memory
 * (USENIX ATC '21)
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "hashtable.h"
#include "smart/task.h"
#include "smart/backoff.h"

thread_local sds::Backoff tl_backoff;

RemoteHashTable::RemoteHashTable(JsonConfig config, int max_threads) : config_(config), is_shared_(false) {
    node_ = new Initiator();
    if (!node_) {
        exit(EXIT_FAILURE);
    }

    node_id_ = 0;
    auto entry = config.get("memory_servers").get(node_id_);
    std::string domain_name = entry.get("hostname").get_str();
    uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
    if (node_->connect(node_id_, domain_name.c_str(), tcp_port, max_threads)) {
        exit(EXIT_FAILURE);
    }

    if (node_->get_root_entry(node_id_, 0, dir_addr_.raw)) {
        exit(EXIT_FAILURE);
    }

    dir_addr_.node = node_id_;
    fast_search_ = node_->config().use_speculative_lookup;
    for (int i = 0; i < kMaxThreads; ++i) {
        for (int j = 0; j < kMaxTasksPerThread; ++j) {
            if (init_task(i, j)) {
                exit(EXIT_FAILURE);
            }
        }
    }
}

RemoteHashTable::RemoteHashTable(JsonConfig config, int max_threads, Initiator *node, int node_id) :
        config_(config), node_(node), node_id_(node_id), is_shared_(true) {
    auto entry = config.get("memory_servers").get(node_id_);
    std::string domain_name = entry.get("hostname").get_str();
    uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
    if (node_->connect(node_id_, domain_name.c_str(), tcp_port, max_threads)) {
        exit(EXIT_FAILURE);
    }

    if (node_->get_root_entry(node_id_, 0, dir_addr_.raw)) {
        exit(EXIT_FAILURE);
    }

    dir_addr_.node = node_id_;
    for (int i = 0; i < kMaxThreads; ++i) {
        for (int j = 0; j < kMaxTasksPerThread; ++j) {
            if (init_task(i, j)) {
                exit(EXIT_FAILURE);
            }
        }
    }
}

RemoteHashTable::~RemoteHashTable() {
    node_->disconnect(node_id_);
    if (!is_shared_) {
        delete node_;
    }
}

int RemoteHashTable::init_task(int thread, int task) {
    TaskLocal &tl = tl_data_[thread][task];
    tl.dir_cache = (Directory *) node_->alloc_cache(sizeof(Directory));
    tl.bucket_buf = (Bucket *) node_->alloc_cache(sizeof(Bucket) * 4);
    tl.block_buf = (BlockHeader *) node_->alloc_cache(kMaxBlockLength);
    tl.cas_buf = (uint64_t *) node_->alloc_cache(sizeof(uint64_t));
    assert(tl.dir_cache && tl.bucket_buf && tl.block_buf && tl.cas_buf);
    do {
        if (node_->read(tl.dir_cache, dir_addr_, sizeof(Directory), Initiator::Option::Sync)) {
            return -1;
        }
    } while (tl.dir_cache->global_depth & kGlobalDepthLockedBit);
    return 0;
}

int RemoteHashTable::search(const std::string &key, std::string &value) {
    if (fast_search(key, value)) {
        return 0;
    }
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    uint32_t hash[2];
    hash_function(key, hash);
    bool retry_flag = false;
retry:
    uint32_t subtable_idx = hash[0] % (1 << tl.dir_cache->global_depth);
    auto &subtable = tl.dir_cache->subtables[subtable_idx];
    BucketDesc bucket[4]; // [h1, h2, h1_overflow, h2_overflow]
    read_bucket(bucket, hash);
    for (int i = 0; i < 4; ++i) {
        for (int j = 0; j < kAssociatedWays; ++j) {
            auto &slot = bucket[i].buf->slots[j];
            if (slot.raw && slot.fp == get_fingerprint(hash[0])) {
                int rc = read_block(key, value, slot);
                if (rc == 0) {
                    if (fast_search_) {
                        key_cache_.add(key, bucket[i].addr, slot.raw);
                    }
                    return 0;
                }
                assert(rc == ENOENT); // retry
            }
        }
    }
    if (!retry_flag) {
        for (int i = 0; i < 4; ++i) {
            uint32_t key_suffix = hash[0] % (1 << bucket[i].buf->local_depth);
            if (bucket[i].buf->local_depth != subtable.local_depth && bucket[i].buf->suffix != key_suffix) {
                retry_flag = true;
                read_directory();
                goto retry;
            }
        }
    }
    return ENOENT;
}

int RemoteHashTable::insert(const std::string &key, const std::string &value) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    uint32_t hash[2];
    hash_function(key, hash);
    BucketDesc bucket[4];   // [h1, h2, h1_overflow, h2_overflow]
    Slot *slot, new_slot, null_slot = {.raw = 0};
    uint64_t slot_addr = 0;
    int slot_bucket;
    BackoffGuard guard(tl_backoff);
    int rc;
    write_block(key, value, new_slot);
    while (true) {
        slot = nullptr;
        uint32_t subtable_idx = hash[0] % (1 << tl.dir_cache->global_depth);
        auto &subtable = tl.dir_cache->subtables[subtable_idx];
        read_bucket(bucket, hash);
        for (int i = 0; !slot && i < 4; ++i) {
            for (int j = 0; !slot && j < kAssociatedWays; ++j) {
                auto &entry = bucket[i].buf->slots[j];
                if (!entry.raw) {
                    slot = &entry;
                    slot_addr = get_slot_addr(bucket[i], j);
                    slot_bucket = i;
                }
            }
        }

        if (!slot) {
#ifdef USE_RESIZE
            global_lock();
            SDS_INFO("%d: resize(%d)", GetThreadID(), subtable_idx);
            resize(subtable_idx);
            global_unlock();
            continue;
#else
            assert(0 && "resizing is not disabled");
#endif
        }

        assert(slot->raw == 0);
        rc = atomic_update_slot(slot_addr, slot, new_slot);
        if (rc == 0) {
            break;
        }

        guard.retry_task();
    }

    int retry_cnt = 0;
    while (true) {
        read_bucket(bucket, hash);
        uint32_t subtable_idx = hash[0] % (1 << tl.dir_cache->global_depth);
        auto &subtable = tl.dir_cache->subtables[subtable_idx];
        auto &buf = bucket[slot_bucket].buf;
        uint32_t key_suffix = hash[0] % (1 << buf->local_depth);
        if (buf->local_depth != subtable.local_depth && buf->suffix != key_suffix) {
            BucketDesc move_to_bucket[4];
            read_directory();
            read_bucket(move_to_bucket, hash);
            Slot *move_to_slot = nullptr;
            uint64_t move_to_slot_addr;
            for (int i = 0; !move_to_slot && i < 4; ++i) {
                for (int j = 0; !move_to_slot && j < kAssociatedWays; ++j) {
                    auto &entry = bucket[i].buf->slots[j];
                    if (!entry.raw) {
                        move_to_slot = &entry;
                        move_to_slot_addr = get_slot_addr(bucket[i], j);
                    }
                }
            }
            if (!move_to_slot) {
                retry_cnt++;
                assert(retry_cnt < 100000);
                SDS_INFO("%d: resize(%d)", GetThreadID(), subtable_idx);
                global_lock();
                resize(subtable_idx);
                global_unlock();
                continue;
            }
            rc = atomic_update_slot(move_to_slot_addr, move_to_slot, new_slot);
            if (rc == 0) {
                atomic_update_slot(slot_addr, slot, null_slot);
            }
        } else {
            std::vector<Slot *> match_slots;
            std::vector<uint64_t> match_slot_offsets;
            Slot *min_slot;
            bool fail = false;
            uint64_t min_slot_offset = UINT64_MAX;
            for (int i = 0; i < 4; ++i) {
                for (int j = 0; j < kAssociatedWays; ++j) {
                    auto &entry = bucket[i].buf->slots[j];
                    if (entry.raw && entry.fp == new_slot.fp) {
                        uint64_t offset = get_slot_addr(bucket[i], j);
                        if (slot_addr != offset) {
                            std::string writable_value;
                            if (read_block(key, writable_value, entry)) {
                                continue;
                            }
                        }
                        if (min_slot_offset != offset) {
                            match_slots.push_back(&entry);
                            match_slot_offsets.push_back(offset);
                            if (min_slot_offset > offset) {
                                min_slot_offset = offset;
                                min_slot = &entry;
                            }
                        }
                    }
                }
            }

            if (match_slots.size() <= 1) {
                return 0;
            }

            for (int i = 0; i < match_slots.size(); ++i) {
                slot = match_slots[i];
                if (slot == min_slot) {
                    continue;
                }

                rc = atomic_update_slot(match_slot_offsets[i], slot, null_slot);
                if (rc != 0) {
                    fail = true;
                    break;
                }
            }

            if (!fail) {
                return 0;
            }

            guard.retry_task();
        }
    }

    assert(0 && "unreachable");
}

int RemoteHashTable::update(const std::string &key, const std::string &value) {
    uint32_t hash[2];
    hash_function(key, hash);
    BucketDesc bucket[4];   // [h1, h2, h1_overflow, h2_overflow]
    uint64_t slot_addr = 0;
    Slot new_slot, *slot;
    BackoffGuard guard(tl_backoff);
    int rc;
    write_block(key, value, new_slot);
    while (true) {
        slot = nullptr;
        read_bucket(bucket, hash, true);
        std::string old_value;
        for (int i = 0; !slot && i < 4; ++i) {
            for (int j = 0; !slot && j < kAssociatedWays; ++j) {
                auto &entry = bucket[i].buf->slots[j];
                if (entry.raw && entry.fp == get_fingerprint(hash[0])) {
                    rc = read_block(key, old_value, entry);
                    if (rc == 0) {
                        slot = &entry;
                        slot_addr = get_slot_addr(bucket[i], j);
                    }
                }
            }
        }
        if (!slot) {
            return ENOENT;
        }
        rc = atomic_update_slot(slot_addr, slot, new_slot);
        if (rc == 0) {
            return 0;
        }
        guard.retry_task();
    }
}

int RemoteHashTable::rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform) {
    uint32_t hash[2];
    hash_function(key, hash);
    BucketDesc bucket[4];   // [h1, h2, h1_overflow, h2_overflow]
    uint64_t slot_addr = 0;
    Slot new_slot, *slot;
    BackoffGuard guard(tl_backoff);
    int rc;

    while (true) {
        slot = nullptr;
        read_bucket(bucket, hash, true);
        std::string old_value;
        for (int i = 0; !slot && i < 4; ++i) {
            for (int j = 0; !slot && j < kAssociatedWays; ++j) {
                auto &entry = bucket[i].buf->slots[j];
                if (entry.raw && entry.fp == get_fingerprint(hash[0])) {
                    rc = read_block(key, old_value, entry);
                    if (rc == 0) {
                        slot = &entry;
                        slot_addr = get_slot_addr(bucket[i], j);
                    }
                }
            }
        }
        if (!slot) {
            return ENOENT;
        }
        auto value = transform(old_value);
        GlobalAddress addr;
        write_block(key, value, new_slot, addr);
        rc = atomic_update_slot(slot_addr, slot, new_slot);
        if (rc == 0) {
            return 0;
        }
        size_t block_len = get_kv_block_len(key, value);
        node_->free_memory(addr, block_len);
        guard.retry_task();
    }
}

int RemoteHashTable::remove(const std::string &key) {
    uint32_t hash[2];
    hash_function(key, hash);
    BucketDesc bucket[4];   // [h1, h2, h1_overflow, h2_overflow]
    std::string value;
    Slot *slot, null_slot = {.raw = 0};
    uint64_t slot_addr = 0;
    BackoffGuard guard(tl_backoff);
    while (true) {
        slot = nullptr;
        read_bucket(bucket, hash, true);
        for (int i = 0; !slot && i < 4; ++i) {
            for (int j = 0; !slot && j < kAssociatedWays; ++j) {
                auto &entry = bucket[i].buf->slots[j];
                if (entry.raw && entry.fp == get_fingerprint(hash[0])) {
                    int rc = read_block(key, value, entry);
                    if (rc == 0) {
                        slot = &entry;
                        slot_addr = get_slot_addr(bucket[i], j);
                    }
                }
            }
        }
        if (!slot) {
            return ENOENT;
        }
        int rc = atomic_update_slot(slot_addr, slot, null_slot);
        if (rc == 0) {
            return 0;
        }
        guard.retry_task();
    }

    assert(0 && "unreachable");
}

int RemoteHashTable::read_bucket(BucketDesc *bucket, const uint32_t *hash, bool suffix_check) {
retry:
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    uint32_t subtable_idx = hash[0] % (1 << tl.dir_cache->global_depth);
    auto &subtable = tl.dir_cache->subtables[subtable_idx];
    for (int i = 0; i < 2; ++i) {
        int bucket_index = (int) (hash[i] % kMainBuckets);
        uint64_t bucket_addr = subtable.pointer + (bucket_index / 2) * 3 * sizeof(Bucket);
        auto bucket_buf = &tl.bucket_buf[i * 2];

        if (bucket_index % 2 == 0) {
            bucket[i].buf = bucket_buf;
            bucket[i].addr = bucket_addr;
            bucket[i + 2].buf = bucket_buf + 1;
            bucket[i + 2].addr = bucket_addr + sizeof(Bucket);
        } else {
            bucket[i].buf = bucket_buf + 1;
            bucket[i].addr = bucket_addr + sizeof(Bucket);
            bucket[i + 2].buf = bucket_buf;
            bucket[i + 2].addr = bucket_addr;
        }

        int rc = node_->read(bucket_buf, GlobalAddress(node_id_, bucket_addr),
                             sizeof(Bucket) * 2);
        assert(!rc);
    }
    int rc = node_->sync();
    assert(!rc);

    if (!suffix_check)
        return 0;

    bool retry_flag = false;
    for (int i = 0; i < 4; ++i) {
        if (bucket[i].buf->local_depth != subtable.local_depth &&
            bucket[i].buf->suffix != (hash[0] % (1 << bucket[i].buf->local_depth))) {
            read_directory();
            retry_flag = true;
            break;
        }
    }

    if (retry_flag) {
        GlobalAddress entry_addr = dir_addr_ + ((char *) &subtable - (char *) tl.dir_cache);
        while (true) {
            rc = node_->read(&subtable, entry_addr, sizeof(uint64_t), Initiator::Option::Sync);
            assert(!rc);
            if (!subtable.lock) {
                break;
            }
        }
        goto retry;
    }

    return 0;
}

int RemoteHashTable::read_directory() {
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    retry:
    rc = node_->read(tl.dir_cache, dir_addr_, sizeof(Directory), Initiator::Option::Sync);
    assert(!rc);
    if (tl.dir_cache->global_depth & kGlobalDepthLockedBit) {
        goto retry;
    }
    return 0;
}

int RemoteHashTable::resize(uint32_t subtable_idx) {
    read_directory();
    int retry_cnt = 0;
    auto &resize_lock = thread_data[GetThreadID()].resize_lock;
    auto &resize_lock_cnt = thread_data[GetThreadID()].resize_lock_cnt;
    while (resize_lock != -1 && resize_lock != GetTaskID()) {
        YieldTask();
    }
    resize_lock = GetTaskID();
    resize_lock_cnt++;
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
retry:
    int depth = tl.dir_cache->subtables[subtable_idx].local_depth;
    int global_depth = tl.dir_cache->global_depth;
    int old_suffix = subtable_idx % (1 << depth);
    int new_suffix = old_suffix + (1 << depth);
    assert(old_suffix < (1 << global_depth));
    SDS_INFO("%d %d, %d %d, %d", old_suffix, new_suffix, depth, global_depth, subtable_idx);
    while (new_suffix >= (1 << global_depth)) {
        resize_directory(global_depth);
        global_depth = tl.dir_cache->global_depth;
    }

    auto &old_entry = tl.dir_cache->subtables[old_suffix];
    auto &new_entry = tl.dir_cache->subtables[new_suffix];
    lock_subtable(old_suffix);
    lock_subtable(new_suffix);
    //SDS_INFO("resize begin: %d -> %d depth %d", old_suffix, new_suffix, depth);
    //debug();

    if (old_entry.local_depth != depth || new_entry.local_depth != depth) {
        retry_cnt++;
        assert(retry_cnt < 10000);
        auto new_local_depth = std::max(std::max(old_entry.local_depth, new_entry.local_depth), (uint64_t) depth);
        fix_local_depth(old_suffix, global_depth, new_local_depth);
        for (int i = 0; i < kTotalBuckets; ++i) {
            move_bucket(old_suffix, new_suffix, depth, i);
        }
        unlock_subtable(old_suffix);
        unlock_subtable(new_suffix);
        goto retry;
    }

    if (old_entry.pointer != new_entry.pointer) {
        fix_local_depth(old_suffix, global_depth, depth);
        for (int i = 0; i < kTotalBuckets; ++i) {
            move_bucket(old_suffix, new_suffix, depth, i);
        }
        unlock_subtable(old_suffix);
        unlock_subtable(new_suffix);
        resize_lock_cnt--;
        if (resize_lock_cnt == 0) {
            resize_lock = false;
        }
        return 0;
    }

    // SDS_INFO("resize begin: %d -> %d depth %d", old_suffix, new_suffix, depth);
    auto new_table_addr = node_->alloc_memory(node_id_, sizeof(SubTable));
    assert(new_table_addr != NULL_GLOBAL_ADDRESS);
    new_entry.local_depth = depth + 1;
    old_entry.local_depth = depth + 1;
    new_entry.pointer = new_table_addr.offset;

    // Initialize new table
    SubTable *old_table, *new_table;
    alloc_subtable(old_table, new_table);
    memset(new_table, 0, sizeof(SubTable));
    for (int i = 0; i < kTotalBuckets; ++i) {
        new_table->buckets[i].local_depth = depth + 1;
        new_table->buckets[i].suffix = new_suffix;
    }
    int rc = node_->read(old_table, GlobalAddress(node_id_, old_entry.pointer), sizeof(SubTable));
    assert(!rc);
    rc = node_->write(new_table, new_table_addr, sizeof(SubTable));
    assert(!rc);
    rc = node_->write(&old_entry, dir_addr_ + ((char *) &old_entry - (char *) tl.dir_cache), 
                      sizeof(uint64_t));
    assert(!rc);
    rc = node_->write(&new_entry, dir_addr_ + ((char *) &new_entry - (char *) tl.dir_cache), 
                      sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
    fix_local_depth(old_suffix, global_depth, depth);
    for (int i = 0; i < kTotalBuckets; ++i) {
        move_bucket(old_suffix, new_suffix, depth, i);
    }

    unlock_subtable(old_suffix);
    unlock_subtable(new_suffix);
    SDS_INFO("resize end: %d -> %d depth %d", old_suffix, new_suffix, depth);
    //debug();
    resize_lock_cnt--;
    if (resize_lock_cnt == 0) {
        resize_lock = false;
    }
    if (thread_data[GetThreadID()].resize_suffix >= 0) {
        uint32_t suffix = thread_data[GetThreadID()].resize_suffix;
        thread_data[GetThreadID()].resize_suffix = -1;
        resize(suffix);
    }
    return 0;
}

void RemoteHashTable::fix_local_depth(int old_suffix, int global_depth, int depth) {
    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc;
    int new_suffix = old_suffix + (1 << depth);
    int max_suffix = (1 << global_depth);
    int step = (1 << (1 + depth));
    for (int suffix = old_suffix + step; suffix < max_suffix; suffix += step) {
        auto &subtable = tl.dir_cache->subtables[suffix];
        SubTableEntry old_val = subtable, new_val;
        if (old_val.local_depth > depth || old_val.lock) {
            continue;
        }
        new_val.pointer = old_val.pointer;
        new_val.local_depth = depth + 1;
        new_val.lock = 0;
        rc = node_->compare_and_swap(&subtable, dir_addr_ + ((char *) &subtable - (char *) tl.dir_cache),
                                     old_val.raw, new_val.raw, Initiator::Option::Sync);
        assert(!rc);
        if (subtable.raw == old_val.raw) {
            subtable.raw = new_val.raw;
        }
    }
    auto pointer = tl.dir_cache->subtables[new_suffix].pointer;
    for (int suffix = new_suffix + step; suffix < max_suffix; suffix += step) {
        auto &subtable = tl.dir_cache->subtables[suffix];
        SubTableEntry old_val = subtable, new_val;
        if (old_val.local_depth > depth || old_val.lock) {
            continue;
        }
        new_val.pointer = pointer;
        new_val.local_depth = depth + 1;
        new_val.lock = 0;
        rc = node_->compare_and_swap(&subtable, dir_addr_ + ((char *) &subtable - (char *) tl.dir_cache),
                                     old_val.raw, new_val.raw, Initiator::Option::Sync);
        assert(!rc);
        if (subtable.raw == old_val.raw) {
            subtable.raw = new_val.raw;
        }
    }
}

void RemoteHashTable::global_lock() {
    return;
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &lock = tl.dir_cache->global_lock;
    GlobalAddress addr = dir_addr_ + ((char *) &lock - (char *) tl.dir_cache);
    int retry_cnt = 0;
retry:
    int rc = node_->compare_and_swap(&lock, addr, 0, 1, Initiator::Option::Sync);
    assert(!rc);
    if (lock == 0) {
        return;
    }
    goto retry;
}

void RemoteHashTable::global_unlock() {
    return;
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &lock = tl.dir_cache->global_lock;
    GlobalAddress addr = dir_addr_ + ((char *) &lock - (char *) tl.dir_cache);
    lock = 0;
    int rc = node_->write(&lock, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
}

void RemoteHashTable::alloc_subtable(SubTable *&old_table, SubTable *&new_table) {
    old_table = thread_data[GetThreadID()].old_table;
    new_table = thread_data[GetThreadID()].new_table;
    if (!old_table) {
        old_table = thread_data[GetThreadID()].old_table = (SubTable *) node_->alloc_cache(sizeof(SubTable));
        assert(old_table);
    }
    if (!new_table) {
        new_table = thread_data[GetThreadID()].new_table = (SubTable *) node_->alloc_cache(sizeof(SubTable));
        assert(new_table);
    }
}

bool RemoteHashTable::lock_subtable(uint32_t suffix, bool try_lock) {
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &subtable = tl.dir_cache->subtables[suffix];
    GlobalAddress addr = dir_addr_ + ((char *) &subtable - (char *) tl.dir_cache);
    uint64_t tsc = rdtsc();
retry:
    if (rdtsc() - tsc > 2400 * 1000 * 1000ull) {
        SDS_INFO("lock %d failed", suffix);
        assert(0);
    }
    SubTableEntry old_entry = subtable;
    SubTableEntry new_entry = subtable;
    old_entry.lock = 0;
    new_entry.lock = 1;
    int rc = node_->compare_and_swap(&subtable, addr,
                                     old_entry.raw, new_entry.raw, Initiator::Option::Sync);
    assert(!rc);
    if (subtable.raw == old_entry.raw) {
        subtable.raw = new_entry.raw;
        SDS_INFO("lock %d", suffix);
        return true;
    }
    if (try_lock) {
        return false;
    } else {
        goto retry;
    }
}

void RemoteHashTable::unlock_subtable(uint32_t suffix) {
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &subtable = tl.dir_cache->subtables[suffix];
    GlobalAddress addr = dir_addr_ + ((char *) &subtable - (char *) tl.dir_cache);
    subtable.lock = 0;
    int rc = node_->write(&subtable, addr, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
    SDS_INFO("unlock %d", suffix);
}

int RemoteHashTable::resize_directory(int old_depth) {
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    int rc = 0;
    uint64_t subtable_idx;
    uint64_t old_global_depth = old_depth;
    uint64_t new_global_depth;
    int retry = 0;
    while (true) {
        retry++;
        assert(retry < 1000);
        new_global_depth = old_global_depth + 1;
        assert(new_global_depth <= kMaxGlobalDepth);
        rc = node_->compare_and_swap(tl.cas_buf, dir_addr_,
                                     old_global_depth, new_global_depth | kGlobalDepthLockedBit);
        assert(!rc);
        rc = node_->read(tl.dir_cache, dir_addr_,sizeof(Directory),
                         Initiator::Option::Sync, IBV_SEND_FENCE);
        assert(!rc);
        if (*tl.cas_buf == old_global_depth) {
            break;
        }
        old_global_depth = (*tl.cas_buf) & (~kGlobalDepthLockedBit);
        if (old_global_depth >= old_depth) {
            return 0;
        }
    }
    subtable_idx = 1ull << old_global_depth;
    tl.dir_cache->global_depth = new_global_depth;
    for (int i = 0; i < subtable_idx; ++i) {
        tl.dir_cache->subtables[i + subtable_idx] = tl.dir_cache->subtables[i];
        tl.dir_cache->subtables[i + subtable_idx].lock = 0;
    }
    rc = node_->write(&tl.dir_cache->subtables[subtable_idx],
                      dir_addr_ + ((char *) &tl.dir_cache->subtables[subtable_idx] - (char *) tl.dir_cache),
                      sizeof(uint64_t) * subtable_idx);
    assert(!rc);
    SDS_INFO("resize global_depth: %ld -> %ld", old_global_depth, new_global_depth);
    rc = node_->write(tl.dir_cache, dir_addr_, sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
    return 0;
}

int RemoteHashTable::move_bucket(int old_suffix, int new_suffix, int depth, int bucket_id) {
    if (thread_data[GetThreadID()].resize_suffix >= 0) {
        return 0;
    }
    auto &tl = tl_data_[GetThreadID()][GetTaskID()];
    auto &old_entry = tl.dir_cache->subtables[old_suffix];
    auto &new_entry = tl.dir_cache->subtables[new_suffix];
    auto old_table = thread_data[GetThreadID()].old_table;
    auto new_table = thread_data[GetThreadID()].new_table;
    auto &old_bucket = old_table->buckets[bucket_id];
    auto &new_bucket = new_table->buckets[bucket_id];
    Slot null_slot = { .raw = 0 };
    int rc;
    if (old_bucket.local_depth != depth) {
        return 0;
    }
    uint64_t old_ld_suf = *((uint64_t *) &old_bucket);
    old_bucket.local_depth++;
    uint64_t new_ld_suf = *((uint64_t *) &old_bucket);
    rc = node_->compare_and_swap(&old_bucket, GlobalAddress(node_id_, old_entry.pointer + ((char *) &old_bucket - (char *) old_table)),
                                 old_ld_suf, new_ld_suf, Initiator::Option::Sync);
    assert(!rc);
    std::vector<Slot *> old_slots, new_slots;
    for (int old_id = 0; old_id < kAssociatedWays; ++old_id) {
        auto *old_slot = &old_bucket.slots[old_id];
        if (!old_slot->raw) continue;
        BlockHeader *block = tl.block_buf;
        auto block_len = old_slot->len * kBlockLengthUnit;
        do {
            rc = node_->read(block, GlobalAddress(node_id_, old_slot->pointer), block_len, Initiator::Option::Sync);
            assert(!rc);
        } while (check_kv_block_crc32(block, block_len));

        uint32_t hash[2];
        std::string key(get_kv_block_key(block), block->key_len);
        hash_function(key, hash);
        if (hash[0] % (1 << (depth + 1)) != new_suffix) {
            continue;
        }

        bool succ = false;
        for (int new_id = 0; new_id < kAssociatedWays; ++new_id) {
            auto new_slot = &new_bucket.slots[new_id];
            if (new_slot->raw) continue;
            uint64_t addr = new_entry.pointer + ((char *) new_slot - (char *) new_table);
            rc = atomic_update_slot(addr, new_slot, *old_slot);
            if (rc == 0) {
                old_slots.push_back(old_slot);
                new_slots.push_back(new_slot);
                succ = true;
                break;
            }
        }

        if (!succ) {
            thread_data[GetThreadID()].resize_suffix = new_suffix;
            break;
        }
    }

    for (int j = 0; j < old_slots.size(); ++j) {
        auto &old_slot = old_slots[j];
        auto &new_slot = new_slots[j];
        uint64_t addr = old_entry.pointer + ((char *) old_slot - (char *) old_table);
        atomic_update_slot(addr, old_slot, null_slot);
        if (old_slot->raw == 0) {
            addr = new_entry.pointer + ((char *) new_slot - (char *) new_table);
            atomic_update_slot(addr, new_slot, null_slot);
        }
    }

    return 0;
}

int RemoteHashTable::read_block(const std::string &key, std::string &value, Slot &slot, bool retry) {
    size_t block_len = slot.len * kBlockLengthUnit;
    assert(block_len);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;

    if (retry) {
        do {
            int rc = node_->read(block, GlobalAddress(node_id_, slot.pointer),
                                 block_len, Initiator::Option::Sync);
            assert(!rc);
        } while (check_kv_block_crc32(block, block_len));
    } else {
        int rc = node_->read(block, GlobalAddress(node_id_, slot.pointer),
                             block_len, Initiator::Option::Sync);
        assert(!rc);
        if (check_kv_block_crc32(block, block_len)) {
            return ENOENT;
        }
    }

    const char *block_key = get_kv_block_key(block);
    const char *block_value = get_kv_block_value(block);
    if (strncmp(block_key, key.c_str(), block->key_len) != 0) {
        return ENOENT;
    }

    value = std::string(block_value, block->value_len);
    return 0;
}

int RemoteHashTable::write_block(const std::string &key, const std::string &value, Slot &slot) {
    size_t block_len = get_kv_block_len(key, value);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;
    GlobalAddress addr = node_->alloc_memory(node_id_, block_len);
    assert(addr != NULL_GLOBAL_ADDRESS);
    block->key_len = key.size();
    block->value_len = value.size();
    strncpy(get_kv_block_key(block), key.c_str(), block->key_len);
    strncpy(get_kv_block_value(block), value.c_str(), block->value_len);
    update_kv_block_crc32(block, block_len);
    int rc = node_->write(block, addr, block_len);
    assert(!rc);
    uint32_t hash[2];
    hash_function(key, hash);
    slot.raw = 0;
    slot.fp = get_fingerprint(hash[0]);
    slot.len = block_len / kBlockLengthUnit;
    slot.pointer = addr.offset;
    return 0;
}

int RemoteHashTable::write_block(const std::string &key, const std::string &value, RemoteHashTable::Slot &slot,
                                 sds::GlobalAddress &addr) {
    size_t block_len = get_kv_block_len(key, value);
    BlockHeader *block = tl_data_[GetThreadID()][GetTaskID()].block_buf;
    addr = node_->alloc_memory(node_id_, block_len);
    assert(addr != NULL_GLOBAL_ADDRESS);
    block->key_len = key.size();
    block->value_len = value.size();
    strncpy(get_kv_block_key(block), key.c_str(), block->key_len);
    strncpy(get_kv_block_value(block), value.c_str(), block->value_len);
    update_kv_block_crc32(block, block_len);
    int rc = node_->write(block, addr, block_len);
    assert(!rc);
    uint32_t hash[2];
    hash_function(key, hash);
    slot.raw = 0;
    slot.fp = get_fingerprint(hash[0]);
    slot.len = block_len / kBlockLengthUnit;
    slot.pointer = addr.offset;
    return 0;
}

int RemoteHashTable::atomic_update_slot(uint64_t addr, Slot *slot, Slot &new_val) {
    uint64_t old_val = slot->raw;
    int rc = node_->compare_and_swap(slot, GlobalAddress(node_id_, addr), old_val, new_val.raw,
                                     Initiator::Option::Sync);
    assert(!rc);
    return old_val == slot->raw ? 0 : EAGAIN;
}

bool RemoteHashTable::fast_search(const std::string &key, std::string &value) {
    if (!fast_search_) {
        return false;
    }

    auto &key_cache = key_cache_;
    uint64_t addr;
    Slot slot;
    if (!key_cache.find(key, addr, slot.raw)) {
        return false;
    }

    TaskLocal &tl = tl_data_[GetThreadID()][GetTaskID()];
    uint32_t hash[2];
    hash_function(key, hash);
    auto *buf = &tl.bucket_buf[0];

    node_->read(buf, GlobalAddress(node_id_, addr), sizeof(Bucket));
    int rc = read_block(key, value, slot, false);
    node_->sync();
    if (rc == ENOENT) {
        key_cache.erase(key);
        return false;
    }
    for (int j = 0; j < kAssociatedWays; ++j) {
        auto &my_slot = buf->slots[j];
        if (my_slot.raw && my_slot.fp == get_fingerprint(hash[0]) && my_slot.raw == slot.raw) {
            return true;
        }
    }
    key_cache.erase(key);
    return false;
}
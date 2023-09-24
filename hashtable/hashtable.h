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

#ifndef SDS_HASHTABLE_H
#define SDS_HASHTABLE_H

#include <sys/param.h>

#include "util/crc.h"
#include "util/murmur.h"
#include "util/generator.h"
#include "smart/common.h"
#include "util/json_config.h"

#include "smart/global_address.h"
#include "smart/generic_cache.h"
#include "smart/initiator.h"
#include "smart/target.h"

using namespace sds;

//#define USE_RESIZE

class RemoteHashTable {
public:
    static const char *Name() { return "HashTable"; }

    static int Setup(Target &target) {
#ifdef USE_RESIZE
        const static uint64_t kInitialDepth = 2;
#else
        const static uint64_t kInitialDepth = kMaxGlobalDepth;
#endif
        size_t dir_size = roundup(sizeof(Directory), kChunkSize);
        size_t subtable_size = roundup(sizeof(SubTable), kChunkSize);
        Directory *dir = (Directory *) target.alloc_chunk(dir_size / kChunkSize);
        if (!dir) return -1;
        memset(dir, 0, sizeof(Directory));
        dir->global_depth = kInitialDepth;
        for (int suffix = 0; suffix < (1 << kInitialDepth); ++suffix) {
            SubTable *subtable = (SubTable *) target.alloc_chunk(subtable_size / kChunkSize);
            if (!subtable) return -1;
            memset(subtable, 0, sizeof(SubTable));
            for (int bucket_id = 0; bucket_id < kTotalBuckets; ++bucket_id) {
                subtable->buckets[bucket_id].local_depth = kInitialDepth;
                subtable->buckets[bucket_id].suffix = suffix;
            }
            dir->subtables[suffix].pointer = target.rel_ptr(subtable).raw;
            dir->subtables[suffix].lock = 0;
            dir->subtables[suffix].local_depth = kInitialDepth;
        }
        target.set_root_entry(0, target.rel_ptr(dir).raw);
        return 0;
    }

    RemoteHashTable(JsonConfig config, int max_threads);

    RemoteHashTable(JsonConfig config, int max_threads, Initiator *node, int node_id);

    ~RemoteHashTable();

    RemoteHashTable(const RemoteHashTable &) = delete;

    RemoteHashTable &operator=(const RemoteHashTable &) = delete;

    int search(const std::string &key, std::string &value);

    int insert(const std::string &key, const std::string &value);

    int update(const std::string &key, const std::string &value);

    int remove(const std::string &key);

    int scan(const std::string &key, size_t count, std::vector<std::string> &value_list) {
        assert(0 && "not supported");
    }

    int rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform);

    std::function<void()> get_poll_task(int &running_tasks) {
        return node_->get_poll_task(running_tasks);
    }

    void run_tasks() {
        node_->run_tasks();
    }

    Initiator *initiator() { return node_; }

private:
    constexpr const static uint32_t kHashSeed[2] = {0xcc9e2d51, 0x1b873593};

    static void hash_function(const std::string &key, uint32_t *hash) {
        hash[0] = MurmurHash3_x86_32(key.c_str(), (int) key.size(), kHashSeed[0]);
        hash[1] = MurmurHash3_x86_32(key.c_str(), (int) key.size(), kHashSeed[1]);
        hash[0] = util::FNVHash64(hash[0]) % UINT32_MAX;
        hash[1] = util::FNVHash64(hash[1]) % UINT32_MAX;
    }

    static uint8_t get_fingerprint(uint32_t hash) {
        return (uint8_t) ((hash >> 24) & UINT8_MAX);
    }

private:
    const static size_t kMaxGlobalDepth = 8;
    const static size_t kMaxSubTables = 1 << kMaxGlobalDepth;
    const static size_t kAssociatedWays = 7;
    const static size_t kMainBuckets = 200000;
    static_assert(kMainBuckets % 2 == 0, "");
    const static size_t kTotalBuckets = kMainBuckets / 2 * 3;

    const static size_t kBlockLengthUnit = 64;
    const static size_t kMaxBlockLength = 4096;

    struct BlockHeader {
        uint32_t key_len;
        uint32_t value_len;
        // char key[];
        // char value[];
        // uint32_t crc;
    };

    const static uint64_t kGlobalDepthLockedBit = 1ull << 63;

    union SubTableEntry {
        struct {
            uint64_t lock: 8;
            uint64_t local_depth: 8;
            uint64_t pointer: 48;
        };
        uint64_t raw;
    };

    struct Directory {
        uint64_t global_depth;
        SubTableEntry subtables[kMaxSubTables];
        uint64_t global_lock;
    };

    union Slot {
        struct {
            uint64_t fp: 8;
            uint64_t len: 8;
            uint64_t pointer: 48;
        };
        uint64_t raw;
    };

    struct Bucket {
        uint32_t local_depth;
        uint32_t suffix;
        Slot slots[kAssociatedWays];
    };

    struct SubTable {
        Bucket buckets[kTotalBuckets];
    };

    struct BucketDesc {
        Bucket *buf;
        uint64_t addr;
    };

    struct TaskLocal {
        uint64_t *cas_buf;
        Directory *dir_cache;
        Bucket *bucket_buf;
        SubTable *resize_buf;
        BlockHeader *block_buf;
    };

    struct ThreadLocal {
        SubTable *old_table = nullptr;
        SubTable *new_table = nullptr;
        int resize_lock = -1;
        int resize_lock_cnt = 0;
        int resize_suffix = -1;
    };

private:
    int init_task(int thread, int task);

    int read_bucket(BucketDesc *bucket, const uint32_t *hash, bool suffix_check = false);

    int read_directory();

    int resize(uint32_t subtable_idx);

    int resize_directory(int old_depth);

    int move_bucket(int old_suffix, int new_suffix, int depth, int bucket_id);

    int read_block(const std::string &key, std::string &value, Slot &slot, bool retry = true);

    int write_block(const std::string &key, const std::string &value, Slot &slot);

    int write_block(const std::string &key, const std::string &value, Slot &slot, GlobalAddress &addr);

    int atomic_update_slot(uint64_t addr, Slot *slot, Slot &new_val);

    bool lock_subtable(uint32_t suffix, bool try_lock = false);

    void unlock_subtable(uint32_t suffix);

    void alloc_subtable(SubTable *&old_table, SubTable *&new_table);

    static inline char *get_kv_block_key(BlockHeader *block) {
        return (char *) &block[1];
    }

    static inline char *get_kv_block_value(BlockHeader *block) {
        return (char *) &block[1] + block->key_len;
    }

    static inline size_t get_kv_block_len(const std::string &key, const std::string &value) {
        return roundup(sizeof(BlockHeader) + key.size() + value.size() + sizeof(uint32_t), kBlockLengthUnit);
    }

    static inline uint32_t calc_crc32(BlockHeader *block, size_t block_len) {
        size_t length = block_len - sizeof(uint32_t);
        return CRC::Calculate(block, length, CRC::CRC_32());
    }

    static inline void update_kv_block_crc32(BlockHeader *block, size_t block_len) {
        uint32_t *crc_field = ((uint32_t *) ((uintptr_t) block + block_len)) - 1;
        *crc_field = calc_crc32(block, block_len);
    }

    static inline int check_kv_block_crc32(BlockHeader *block, size_t block_len) {
        uint32_t crc_field = ((uint32_t *) ((uintptr_t) block + block_len))[-1];
        if (crc_field != calc_crc32(block, block_len)) {
            return -1;
        } else {
            return 0;
        }
    }

    static inline uint64_t get_slot_addr(BucketDesc &bucket, int slot_idx) {
        return bucket.addr + sizeof(uint64_t) + slot_idx * sizeof(Slot);
    }

    inline void debug() {
        auto &tl = tl_data_[GetThreadID()][GetTaskID()];
        SDS_INFO("GD=%ld", tl.dir_cache->global_depth);
        for (int i = 0; i < (1 << tl.dir_cache->global_depth); ++i) {
            printf("%d=%ld:%ld:%lx ", i, tl.dir_cache->subtables[i].lock, tl.dir_cache->subtables[i].local_depth, tl.dir_cache->subtables[i].pointer);
        }
        printf("\n");
    }

    void fix_local_depth(int old_suffix, int global_depth, int depth);

    void global_lock();

    void global_unlock();

private:
    JsonConfig config_;
    Initiator *node_;
    int node_id_;
    bool is_shared_;
    GlobalAddress dir_addr_;
    alignas(128) TaskLocal tl_data_[kMaxThreads][kMaxTasksPerThread];
    alignas(128) ThreadLocal thread_data[kMaxThreads];

private:
    class KeyCache {
    public:
        void add(const std::string &key, uint64_t addr, uint64_t slot) {
            Entry value;
            value.addr = addr;
            value.slot = slot;
            cache_.add(key, value);
        }

        bool find(const std::string &key, uint64_t &addr, uint64_t &slot) {
            Entry value;
            if (cache_.find(key, value)) {
                addr = value.addr;
                slot = value.slot;
                return true;
            } else {
                return false;
            }
        }

        void erase(const std::string &key) {
            cache_.erase(key);
        }

    private:
        struct Entry {
            uint64_t addr, slot;
        };
        GenericCache<std::string, Entry> cache_;
    };

    KeyCache key_cache_;
    bool fast_search_;

    bool fast_search(const std::string &key, std::string &value);
};

class RemoteHashTableMultiShard {
    constexpr const static uint32_t kHashSeed = 0x1b873593;

    RemoteHashTable *get_shard(const std::string &key) {
        uint32_t hash = MurmurHash3_x86_32(key.c_str(), (int) key.size(), kHashSeed);
        return shard_list_[hash % nr_shards_];
    }

public:
    static const char *Name() { return "HashTableMultiShard"; }

    RemoteHashTableMultiShard(JsonConfig config, int max_threads) {
        node_ = new Initiator();
        if (!node_) {
            exit(EXIT_FAILURE);
        }
        nr_shards_ = (int) config.get("memory_servers").size();
        if (getenv("MEMORY_NODES")) {
            nr_shards_ = std::min(nr_shards_, atoi(getenv("MEMORY_NODES")));
        }
        shard_list_.resize(nr_shards_);
        for (int i = 0; i < nr_shards_; ++i) {
            shard_list_[i] = new RemoteHashTable(config, max_threads, node_, i);
        }
    }

    ~RemoteHashTableMultiShard() {
        for (auto &shard : shard_list_) {
            delete shard;
        }
        delete node_;
    }

    RemoteHashTableMultiShard(const RemoteHashTableMultiShard &) = delete;

    RemoteHashTableMultiShard &operator=(const RemoteHashTableMultiShard &) = delete;

    int search(const std::string &key, std::string &value) {
        return get_shard(key)->search(key, value);
    }

    int insert(const std::string &key, const std::string &value) {
        return get_shard(key)->insert(key, value);
    }

    int update(const std::string &key, const std::string &value) {
        return get_shard(key)->update(key, value);
    }

    int remove(const std::string &key) {
        return get_shard(key)->remove(key);
    }

    int scan(const std::string &key, size_t count, std::vector<std::string> &value_list) {
        assert(0 && "not supported");
    }

    int rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform) {
        return get_shard(key)->rmw(key, transform);
    }

    std::function<void()> get_poll_task(int &running_tasks) {
        return node_->get_poll_task(running_tasks);
    }

    void run_tasks() {
        node_->run_tasks();
    }

    Initiator *initiator() { return node_; }

private:
    Initiator *node_;
    int nr_shards_;
    std::vector<RemoteHashTable *> shard_list_;
};

#endif //SDS_HASHTABLE_H

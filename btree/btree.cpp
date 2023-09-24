// Some contents of this file are derived from Sherman 
// https://github.com/thustorage/Sherman

#include "util/crc.h"
#include "util/json_config.h"
#include "btree.h"
#include "smart/backoff.h"
#include "smart/config.h"

// #define USE_HOCL
#define LEAF_PAGE_FLAG (1u << 31u)

bool g_use_hocl = true;

RemoteBPlusTree::RemoteBPlusTree(JsonConfig config, int max_threads) : config_(config) {
    node_ = new Initiator();
    if (!node_) {
        exit(EXIT_FAILURE);
    }

    auto memory_servers = config.get("memory_servers");
    nr_shards_ = (int) memory_servers.size();
    if (getenv("MEMORY_NODES")) {
        nr_shards_ = std::min(nr_shards_, atoi(getenv("MEMORY_NODES")));
    }
    for (int node_id = 0; node_id < nr_shards_; ++node_id) {
        auto entry = memory_servers.get(node_id);
        std::string domain_name = entry.get("hostname").get_str();
        uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
        if (node_->connect(node_id, domain_name.c_str(), tcp_port, max_threads)) {
            exit(EXIT_FAILURE);
        }
    }

    fast_search_ = node_->config().use_speculative_lookup;
    GlobalAddress value;
    if (node_->get_root_entry(0, 0, value.raw)) {
        exit(EXIT_FAILURE);
    }

    if (getenv("USE_HOCL")) {
        int enable = atoi(getenv("USE_HOCL"));
        if (enable)
            g_use_hocl = true;
        else
            g_use_hocl = false;
    }

    root_ptr_addr_ = RemoteAddress(value.node, value.offset);
    local_locks_ = new LocalLock *[nr_shards_];
    for (int i = 0; i < nr_shards_; ++i) {
        local_locks_[i] = new LocalLock[kNumOfLock];
    }

    const static size_t kIndexCacheSize = 1024;
    index_cache_ = new IndexCache(kIndexCacheSize);
    cas_buf_ = (uint64_t *) node_->alloc_cache(kMaxThreads * kMaxTasksPerThread * sizeof(uint64_t));
    page_buf_[0] = (char *) node_->alloc_cache(kMaxThreads * kMaxTasksPerThread * kBTreePageSize);
    page_buf_[1] = (char *) node_->alloc_cache(kMaxThreads * kMaxTasksPerThread * kBTreePageSize);
    raw_buf_[0] = (char *) node_->alloc_cache(kMaxThreads * kMaxTasksPerThread * kRawPageSize);
    raw_buf_[1] = (char *) node_->alloc_cache(kMaxThreads * kMaxTasksPerThread * kRawPageSize);
    if (!cas_buf_ || !page_buf_[0] || !page_buf_[1] || !raw_buf_[0] || !raw_buf_[1]) {
        exit(EXIT_FAILURE);
    }
    stack_ = new PageStack[kMaxThreads * kMaxTasksPerThread];
    root_ptr_ = get_root_ptr();
}

RemoteBPlusTree::~RemoteBPlusTree() {
    for (int node_id = 0; node_id < nr_shards_; ++node_id) {
        node_->disconnect(node_id);
    }
    delete node_;
    for (int i = 0; i < nr_shards_; ++i) {
        delete[] local_locks_[i];
    }
    delete[]local_locks_;
    delete[]index_cache_;
    delete[]stack_;
}

using namespace sds;
thread_local sds::Backoff tl_backoff;

void RemoteBPlusTree::HOCL_lock(rem_addr_t lock_addr) {
    if (g_use_hocl) {
        auto &lock = local_locks_[GetNodeID(lock_addr)][GetOffset(lock_addr) / sizeof(uint64_t)];
        uint64_t lock_val = lock.val.fetch_add(1);  // add ticket
        uint32_t ticket = lock_val & UINT32_MAX;
        uint32_t current = lock_val >> 32;
        while (ticket != current) {
            YieldTask();
            current = lock.val.load(std::memory_order_relaxed) >> 32;
        }
        if (lock.handover) {
            return;
        }
        uint64_t *buf = get_cas_buf();
        int retry_cnt = 0;
        while (true) {
            int rc = node_->compare_and_swap(buf, ToGlobalAddress(lock_addr, true),
                                             0, 1, Initiator::Option::Sync);
            assert(!rc);
            if (*buf == 0) {
                return;
            }
            retry_cnt++;
            assert(retry_cnt < 1000000);
        }
    } else {
        uint64_t *buf = get_cas_buf();
        int retry_cnt = 0;
        while (true) {
            int rc = node_->compare_and_swap(buf, ToGlobalAddress(lock_addr, true),
                                             0, 1, Initiator::Option::Sync);
            assert(!rc);
            if (*buf == 0) {
                return;
            }
            retry_cnt++;
            assert(retry_cnt < 1000000);
            tl_backoff.retry_task();
        }
    }
}

void RemoteBPlusTree::HOCL_unlock(rem_addr_t lock_addr) {
    if (g_use_hocl) {
        const static size_t kMaxHandOverDepth = 8;
        auto &lock = local_locks_[GetNodeID(lock_addr)][GetOffset(lock_addr) / sizeof(uint64_t)];
        uint64_t lock_val = lock.val.load(std::memory_order_relaxed);
        uint32_t ticket = lock_val & UINT32_MAX;
        uint32_t current = lock_val >> 32;
        if (ticket <= current + 1) {            // no pending locks
            lock.handover = false;
        } else {
            lock.handover = lock.handover_depth < kMaxHandOverDepth;
            lock.handover_depth++;
        }
        if (!lock.handover) {
            lock.handover_depth = 0;
            uint64_t *buf = get_cas_buf();
            *buf = 0;
            int rc = node_->write(buf, ToGlobalAddress(lock_addr, true),
                                sizeof(uint64_t), Initiator::Option::Sync);
            assert(!rc);
        }
        lock.val.fetch_add(1ull << 32);         // add current
    } else {
        uint64_t *buf = get_cas_buf();
        *buf = 0;
        int rc = node_->write(buf, ToGlobalAddress(lock_addr, true),
                            sizeof(uint64_t), Initiator::Option::Sync);
        assert(!rc);
    }
}

rem_addr_t RemoteBPlusTree::get_root_ptr() {
    auto buf = get_cas_buf();
    int rc = node_->read(buf, ToGlobalAddress(root_ptr_addr_), sizeof(uint64_t), Initiator::Option::Sync);
    assert(!rc);
    root_ptr_ = *buf;
    return *buf;
}

bool RemoteBPlusTree::update_root_ptr(rem_addr_t old_root_addr, rem_addr_t new_root_addr) {
    auto buf = get_cas_buf();
    int rc = node_->compare_and_swap(buf, ToGlobalAddress(root_ptr_addr_),
                                     old_root_addr, new_root_addr, Initiator::Option::Sync);
    assert(!rc);
    if (*buf == old_root_addr) {
        root_ptr_ = new_root_addr;
        SDS_INFO("update root ptr %lx -> %lx", old_root_addr, new_root_addr);
        return true;
    } else {
        return false;
    }
}

rem_addr_t RemoteBPlusTree::alloc_page() {
    thread_local int next_node_id = 0;
    int node_id = next_node_id;
    next_node_id = (next_node_id + 1) % nr_shards_;
    auto res = node_->alloc_memory(node_id, kRawPageSize);
    assert(res.offset && res.offset % 64 == 0);
    return RemoteAddress(node_id, res.offset);
}

int RemoteBPlusTree::search(const Key &key, std::string &value) {
    BackoffGuard guard(tl_backoff);
    if (fast_search(key, value)) {
        return 0;
    }
    rem_addr_t addr = root_ptr_;
    SearchResult result;
    auto entry = index_cache_->lookup(key, addr);
    bool cache_hit = (entry != nullptr);
    while (true) {
        if (!do_page_search(addr, key, result, cache_hit)) {
            if (cache_hit) {
                index_cache_->invalidate(entry);
                cache_hit = false;
                addr = get_root_ptr();
            }
            continue;
        }
        if (result.is_leaf()) {
            if (result.valid_bit) {
                if (fast_search_) {
                    // key_cache_[GetThreadID()].add(key, result.page_addr, result.entry_addr);
                    key_cache_.add(key, result.page_addr, result.entry_addr);
                }
                value = std::string(result.val, kValueLength);
                return 0;
            } else if (result.slibing) {
                addr = result.slibing;
                continue;
            } else {
                return ENOENT;
            }
        } else {
            addr = result.slibing ? result.slibing : result.next_level;
            continue;
        }
    }
}

bool RemoteBPlusTree::fast_search(const Key &key, std::string &value) {
    if (!fast_search_) {
        return false;
    }
    auto &key_cache = key_cache_; // key_cache_[GetThreadID()];
    rem_addr_t page_addr, entry_addr;
    if (!key_cache.find(key, page_addr, entry_addr)) {
        return false;
    }
    uint64_t page_off = (uint64_t) entry_addr - (uint64_t) page_addr;
    if (page_off / 60 != (page_off + sizeof(LeafEntry) - 1) / 60) {
        key_cache.erase(key);
        return false;
    }
    uint32_t *raw_buf = get_raw_buf();
    uint64_t raw_off = page_off / 60 * 64;
    int rc = node_->read(raw_buf, page_addr + raw_off,64, Initiator::Option::Sync);
    assert(!rc);
    if (!(raw_buf[0] & LEAF_PAGE_FLAG)) {
        key_cache.erase(key);
        return false;
    }
    auto entry = (LeafEntry *) ((char *) raw_buf + 4 + page_off % 60);
    if (entry->key == key && entry->valid_bit) {
        value = std::string(entry->value, kValueLength);
        RecordCacheHit();
        return true;
    }
    return false;
}

bool RemoteBPlusTree::fast_upsert(const Key &key, const std::string &value) {
    return false;
    auto &key_cache = key_cache_; // key_cache_[GetThreadID()];
    rem_addr_t page_addr, entry_addr;
    if (!key_cache.find(key, page_addr, entry_addr)) {
        key_cache.erase(key);
        return false;
    }
    uint64_t page_off = (uint64_t) entry_addr - (uint64_t) page_addr;
    if (page_off / 60 != (page_off + sizeof(LeafEntry) - 1) / 60) {
        key_cache.erase(key);
        return false;
    }
    uint32_t *raw_buf = get_raw_buf();
    uint64_t raw_off = page_off / 60 * 64;
    rem_addr_t lock_addr = get_lock_addr(page_addr);
    HOCL_lock(lock_addr);
    int rc = node_->read(raw_buf, page_addr + raw_off,64, Initiator::Option::Sync);
    assert(!rc);
    if (!(raw_buf[0] & LEAF_PAGE_FLAG)) {
        key_cache.erase(key);
        HOCL_unlock(lock_addr);
        return false;
    }
    auto entry = (LeafEntry *) ((char *) raw_buf + 4 + page_off % 60);
    if (entry->key != key || !entry->valid_bit) {
        key_cache.erase(key);
        HOCL_unlock(lock_addr);
        return false;
    }
    memcpy(entry->value, value.c_str(), kValueLength);
    rc = node_->write(entry, page_addr + raw_off + 4 + page_off % 60,
                      sizeof(LeafEntry), Initiator::Option::Sync);
    assert(!rc);
    HOCL_unlock(lock_addr);
    return true;
}

bool RemoteBPlusTree::do_page_search(rem_addr_t page_addr, const Key &key, SearchResult &result, bool cache_hit) {
    auto page_buf = get_page_buf();
    auto raw_buf = get_raw_buf();
    read_page(page_addr, page_buf, raw_buf);
    memset(&result, 0, sizeof(result));
    Header *hdr = &((LeafPage *) page_buf)->hdr;
    result.level = hdr->level;
    bool is_leaf = result.is_leaf();
    if (is_leaf) {
        auto page = (LeafPage *) page_buf;
        if (cache_hit && (key < page->hdr.min_key || key >= page->hdr.max_key)) {
            return false;
        }
        assert(key >= page->hdr.min_key);
        if (key >= page->hdr.max_key) {
            result.slibing = page->hdr.sibling;
            return true;
        }
        leaf_page_search(page, page_addr, key, result);
    } else {
        assert(result.level != 0);
        auto page = (InternalPage *) page_buf;
        if (result.level == 1) {
            index_cache_->add(page);
        }
        if (key >= page->hdr.max_key) {
            result.slibing = page->hdr.sibling;
            return true;
        }
        assert(key >= page->hdr.min_key);
        internal_page_search(page, key, result);
    }
    return true;
}

void RemoteBPlusTree::read_page(rem_addr_t page_addr, void *page_buf, uint32_t *raw_buf) {
    assert(page_addr);
    int retry = 0;
    int rc;
    while (true) {
        retry++;
        assert(retry < 100000);
        rc = node_->read(raw_buf, page_addr, kRawPageSize, Initiator::Option::Sync);
        assert(!rc);
        uint32_t psn = raw_buf[0];
        int data_off = 0, buf_off = 0;
        bool fail = false;
        for (; data_off * sizeof(uint32_t) < kBTreePageSize; data_off += 15, buf_off += 16) {
            if (raw_buf[buf_off] != psn) {
                fail = true;
                break;
            }
            memcpy(((uint32_t *) page_buf) + data_off, raw_buf + buf_off + 1, 60);
        }
        if (!fail) {
            auto hdr = (Header *) page_buf;
            if (hdr->level != 0) {
                // assert(hdr->ver_lock ==
                //        CRC::Calculate(&hdr[1], kBTreePageSize - sizeof(Header), CRC::CRC_32()));
            }
            return;
        }
    }
}

void RemoteBPlusTree::write_page(rem_addr_t page_addr, void *page_buf, uint32_t *raw_buf, bool sync) {
    int rc;
    int data_off = 0, buf_off = 0;
    uint32_t psn;
    auto hdr = (Header *) page_buf;
    if (hdr->level == 0) {
        psn = (raw_buf[0] + 1) % LEAF_PAGE_FLAG + LEAF_PAGE_FLAG;
    } else {
        psn = (raw_buf[0] + 1) % LEAF_PAGE_FLAG;
        // hdr->ver_lock = CRC::Calculate(&hdr[1], kBTreePageSize - sizeof(Header), CRC::CRC_32());
    }
    for (; data_off * sizeof(uint32_t) < kBTreePageSize; data_off += 15, buf_off += 16) {
        raw_buf[buf_off] = psn;
        memcpy(raw_buf + buf_off + 1, ((uint32_t *) page_buf) + data_off, 60);
    }
    rc = node_->write(raw_buf,ToGlobalAddress(page_addr),kRawPageSize);
    assert(!rc);
    if (sync) {
        rc = node_->sync();
        assert(!rc);
    }
}

void RemoteBPlusTree::write_leaf_entry(rem_addr_t page_addr, LeafPage *page, LeafEntry *entry) {
    uint64_t page_off = (uint64_t) entry - (uint64_t) page;
    if (page_off / 60 != (page_off + sizeof(LeafEntry) - 1) / 60) {
        write_page(page_addr, page, get_raw_buf(), true);
    } else {
        uint64_t raw_off = page_off / 60 * 64 + 4 + page_off % 60;
        int rc = node_->write(entry, page_addr + raw_off,
                              sizeof(LeafEntry), Initiator::Option::Sync);
        assert(!rc);
    }
}

void RemoteBPlusTree::internal_page_search(InternalPage *page, const Key &key, SearchResult &result) {
    auto last_index = page->hdr.last_index;
    if (key < page->records[0].key) {
        result.next_level = page->hdr.leftmost;
        return;
    }
    for (int i = 1; i <= last_index; ++i) {
        if (key < page->records[i].key) {
            result.next_level = page->records[i - 1].ptr;
            return;
        }
    }
    result.next_level = page->records[last_index].ptr;
}

void RemoteBPlusTree::leaf_page_search(LeafPage *page, rem_addr_t page_addr, const Key &key, SearchResult &result) {
    for (int i = 0; i < kLeafCardinality; ++i) {
        auto &rec = page->records[i];
        if (rec.valid_bit && rec.key == key) {
            memcpy(result.val, rec.value, kValueLength);
            result.valid_bit = true;
            if (fast_search_) {
                result.page_addr = page_addr;
                result.entry_addr = page_addr + ((uint64_t) &rec - (uint64_t) page);
            }
            return;
        }
    }
}

int RemoteBPlusTree::scan(const Key &key, size_t count, std::vector<std::string> &value_list) {
    BackoffGuard guard(tl_backoff);
    rem_addr_t addr = root_ptr_;
    SearchResult result;
    auto entry = index_cache_->lookup(key, addr);
    bool cache_hit = (entry != nullptr);
    value_list.clear();
    while (true) {
        if (!do_page_search(addr, key, result, cache_hit)) {
            if (cache_hit) {
                index_cache_->invalidate(entry);
                cache_hit = false;
                addr = get_root_ptr();
            }
            continue;
        }
        if (result.is_leaf()) {
            while (true) {
                auto page = (LeafPage *) get_page_buf();
                for (int i = 0; i < kLeafCardinality; ++i) {
                    if (page->records[i].valid_bit && page->records[i].key >= key) {
                        value_list.push_back(std::string(page->records[i].value, kValueLength));
                        if (value_list.size() == count) {
                            return 0;
                        }
                    }
                }
                if (!page->hdr.sibling) {
                    return 0;
                }
                read_page(page->hdr.sibling, page, get_raw_buf());
            }
        } else {
            addr = result.slibing ? result.slibing : result.next_level;
            continue;
        }
    }
}

int RemoteBPlusTree::rmw(const Key &key, const std::function<std::string(const std::string &)> &transform) {
    BackoffGuard guard(tl_backoff);
    clear_path_stack();
    rem_addr_t root_addr = root_ptr_, addr;
    auto entry = index_cache_->lookup(key, addr);
    if (entry) {
        if (leaf_page_rmw(addr, key, transform, root_addr, true)) {
            return 0;
        }
        index_cache_->invalidate(entry);
    }
    SearchResult result;
    addr = root_addr;
    int retry_cnt = 0;
    while (true) {
        retry_cnt++;
        assert(retry_cnt < 100000);
        do_page_search(addr, key, result);
        set_path_stack(result.level, addr);
        // SDS_INFO("%lx %d %lx", key, result.level, addr);
        if (!result.is_leaf()) {
            if (result.slibing) {
                addr = result.slibing;
                continue;
            }
            addr = result.next_level;
            if (result.level != 1) {
                continue;
            }
        }
        assert(addr);
        if (leaf_page_rmw(addr, key, transform, root_addr, false)) {
            return 0;
        } else {
            addr = get_root_ptr();
            root_addr = addr;
            continue;
        }
    }
}

int RemoteBPlusTree::upsert(const Key &key, const std::string &value) {
    BackoffGuard guard(tl_backoff);
    clear_path_stack();
    rem_addr_t root_addr = root_ptr_, addr;
    auto entry = index_cache_->lookup(key, addr);
    if (entry) {
        if (leaf_page_store(addr, key, value, root_addr, true)) {
            return 0;
        }
        index_cache_->invalidate(entry);
    }
    SearchResult result;
    addr = root_addr;
    while (true) {
        do_page_search(addr, key, result);
        set_path_stack(result.level, addr);
        // SDS_INFO("%lx %d %lx", key, result.level, addr);
        if (!result.is_leaf()) {
            if (result.slibing) {
                addr = result.slibing;
                continue;
            }
            addr = result.next_level;
            if (result.level != 1) {
                continue;
            }
        }
        assert(addr);
        if (leaf_page_store(addr, key, value, root_addr, false)) {
            return 0;
        } else {
            addr = get_root_ptr();
            root_addr = addr;
            continue;
        }
    }
}

bool RemoteBPlusTree::leaf_page_store(rem_addr_t page_addr, const Key &key, const std::string &value,
                                      rem_addr_t root_addr, bool cache_hit) {
    rem_addr_t lock_addr = get_lock_addr(page_addr);
    HOCL_lock(lock_addr);
    auto page = (LeafPage *) get_page_buf();
    auto raw_buf = get_raw_buf();
    read_page(page_addr, page, raw_buf);
    assert(page->hdr.level == 0);
    if (cache_hit && (key < page->hdr.min_key || key >= page->hdr.max_key)) {
        HOCL_unlock(lock_addr);
        return false;
    }
    if (key >= page->hdr.max_key) {
        HOCL_unlock(lock_addr);
        assert(page->hdr.sibling);
        return leaf_page_store(page->hdr.sibling, key, value, root_addr, false);
    }
    assert(key >= page->hdr.min_key);
    int cnt = 0;
    int empty_index = -1;
    LeafEntry *target_entry = nullptr;
    for (int i = 0; i < kLeafCardinality; ++i) {
        auto &rec = page->records[i];
        if (rec.valid_bit) {
            cnt++;
            if (rec.key == key) {
                memcpy(rec.value, value.c_str(), kValueLength);
                target_entry = &rec;
                break;
            }
        } else if (empty_index == -1) {
            empty_index = i;
        }
    }

    assert(cnt != kLeafCardinality);
    if (target_entry == nullptr) {
        assert(empty_index != -1);
        auto &rec = page->records[empty_index];
        rec.key = key;
        memcpy(rec.value, value.c_str(), kValueLength);
        rec.valid_bit = 1;
        target_entry = &rec;
        cnt++;
    }

    bool need_split = cnt == kLeafCardinality;
    if (!need_split) {
        assert(target_entry);
        write_leaf_entry(page_addr, page, target_entry);
        HOCL_unlock(lock_addr);
        return true;
    }
    std::sort(page->records, page->records + kLeafCardinality,
              [](const LeafEntry &lhs, const LeafEntry &rhs) { return lhs.key < rhs.key; });
    Key split_key;
    rem_addr_t sibling_addr;
    sibling_addr = alloc_page();
    auto sibling = new(get_page_buf(1)) LeafPage();
    sibling->hdr.level = page->hdr.level;
    int m = cnt / 2;
    split_key = page->records[m].key;
    assert(split_key > page->hdr.min_key);
    assert(split_key < page->hdr.max_key);
    for (int i = m; i < cnt; ++i) {
        sibling->records[i - m].key = page->records[i].key;
        sibling->records[i - m].valid_bit = 1;
        memcpy(sibling->records[i - m].value, page->records[i].value, kValueLength);
        page->records[i].key = 0;
        page->records[i].valid_bit = 0;
    }
    page->hdr.last_index -= (cnt - m);
    sibling->hdr.last_index += (cnt - m);
    sibling->hdr.min_key = split_key;
    sibling->hdr.max_key = page->hdr.max_key;
    page->hdr.max_key = split_key;
    sibling->hdr.sibling = page->hdr.sibling;
    page->hdr.sibling = sibling_addr;
    memset(get_raw_buf(1), 0, kRawPageSize);
    write_page(sibling_addr, sibling, get_raw_buf(1));
    write_page(page_addr, page, raw_buf, true);
    HOCL_unlock(lock_addr);
    // SDS_INFO("split leaf: %lx[%lx-%lx], %lx[%lx-%lx]", page_addr, page->hdr.min_key, page->hdr.max_key,
    //          sibling_addr, sibling->hdr.min_key, sibling->hdr.max_key);
    if (root_addr == page_addr) {
        return update_new_root(page_addr, split_key, sibling_addr, 1, root_addr);
    }
    auto parent_addr = get_path_stack(1);
    if (parent_addr) {
        return internal_page_store(parent_addr, split_key, sibling_addr, root_addr, 1);
    } else {
        return insert_internal(split_key, sibling_addr, 1);
    }
}

bool RemoteBPlusTree::leaf_page_rmw(rem_addr_t page_addr, const Key &key,
                                    const std::function<std::string(const std::string &)> &transform,
                                    rem_addr_t root_addr, bool cache_hit) {
    rem_addr_t lock_addr = get_lock_addr(page_addr);
    int rc;
    HOCL_lock(lock_addr);
    auto page = (LeafPage *) get_page_buf();
    auto raw_buf = get_raw_buf();
    read_page(page_addr, page, raw_buf);
    assert(page->hdr.level == 0);
    if (cache_hit && (key < page->hdr.min_key || key >= page->hdr.max_key)) {
        HOCL_unlock(lock_addr);
        return false;
    }
    if (key >= page->hdr.max_key) {
        HOCL_unlock(lock_addr);
        assert(page->hdr.sibling);
        return leaf_page_rmw(page->hdr.sibling, key, transform, root_addr, false);
    }
    assert(key >= page->hdr.min_key);
    LeafEntry *target_entry = nullptr;
    for (int i = 0; i < kLeafCardinality; ++i) {
        auto &rec = page->records[i];
        if (rec.valid_bit && rec.key == key) {
            std::string from = std::string(rec.value, kValueLength);
            std::string to = transform(from);
            memcpy(rec.value, to.c_str(), kValueLength);
            target_entry = &rec;
            break;
        }
    }
    if (target_entry == nullptr) {
        HOCL_unlock(lock_addr);
        return true;
    }
    write_leaf_entry(page_addr, page, target_entry);
    HOCL_unlock(lock_addr);
    return true;
}

bool RemoteBPlusTree::update_new_root(rem_addr_t left, const Key &key, rem_addr_t right, int level,
                                      rem_addr_t old_root_addr) {
    auto new_root = new(get_page_buf()) InternalPage(left, key, right, level);
    auto new_root_addr = alloc_page();
    write_page(new_root_addr, new_root, get_raw_buf());
    return update_root_ptr(old_root_addr, new_root_addr);
}

bool RemoteBPlusTree::insert_internal(const Key &key, rem_addr_t value, int level) {
    auto root = get_root_ptr();
    SearchResult result;
    auto addr = root;
    while (true) {
        do_page_search(addr, key, result);
        set_path_stack(result.level, addr);
        assert(result.level != 0);
        if (result.level == level) {
            return internal_page_store(addr, key, value, root, level);
        }
        addr = result.slibing ? result.slibing : result.next_level;
    }
}

bool
RemoteBPlusTree::internal_page_store(rem_addr_t page_addr, const Key &key, rem_addr_t value, rem_addr_t root,
                                     int level) {
    assert(page_addr);
    rem_addr_t lock_addr = get_lock_addr(page_addr);
    HOCL_lock(lock_addr);
    auto page = (InternalPage *) get_page_buf();
    auto raw_buf = get_raw_buf();
    read_page(page_addr, page, raw_buf);
    assert(page->hdr.level == level);
    if (key >= page->hdr.max_key) {
        HOCL_unlock(lock_addr);
        assert(page->hdr.sibling);
        return internal_page_store(page->hdr.sibling, key, value, root, level);
    }
    assert(key >= page->hdr.min_key);
    auto cnt = page->hdr.last_index + 1;
    bool is_update = false;
    uint16_t insert_index = 0;
    for (int i = cnt - 1; i >= 0; --i) {
        if (page->records[i].key == key) {
            page->records[i].ptr = value;
            is_update = true;
            break;
        }
        if (page->records[i].key < key) {
            insert_index = i + 1;
            break;
        }
    }
    assert(cnt != kInternalCardinality);
    if (!is_update) {
        for (int i = cnt; i > insert_index; --i) {
            page->records[i].key = page->records[i - 1].key;
            page->records[i].ptr = page->records[i - 1].ptr;
        }
        page->records[insert_index].key = key;
        page->records[insert_index].ptr = value;
        page->hdr.last_index++;
    }
    cnt = page->hdr.last_index + 1;
    bool need_split = cnt == kInternalCardinality;
    Key split_key;
    rem_addr_t sibling_addr;
    if (need_split) {
        sibling_addr = alloc_page();
        auto sibling = new(get_page_buf(1)) InternalPage(page->hdr.level);
        int m = cnt / 2;
        split_key = page->records[m].key;
        assert(split_key > page->hdr.min_key);
        assert(split_key < page->hdr.max_key);
        for (int i = m + 1; i < cnt; ++i) { // move
            sibling->records[i - m - 1].key = page->records[i].key;
            sibling->records[i - m - 1].ptr = page->records[i].ptr;
        }
        page->hdr.last_index -= (cnt - m);
        sibling->hdr.last_index += (cnt - m - 1);
        sibling->hdr.leftmost = page->records[m].ptr;
        sibling->hdr.min_key = page->records[m].key;
        sibling->hdr.max_key = page->hdr.max_key;
        page->hdr.max_key = page->records[m].key;
        sibling->hdr.sibling = page->hdr.sibling;
        page->hdr.sibling = sibling_addr;
        memset(get_raw_buf(1), 0, kRawPageSize);
        write_page(sibling_addr, sibling, get_raw_buf(1));
        // SDS_INFO("split internal: %lx[%lx-%lx], %lx[%lx-%lx]", page_addr, page->hdr.min_key, page->hdr.max_key,
        //          sibling_addr, sibling->hdr.min_key, sibling->hdr.max_key);
    }
    write_page(page_addr, page, raw_buf, true);
    HOCL_unlock(lock_addr);
    if (!need_split) {
        return true;
    }
    if (root == page_addr) {
        return update_new_root(page_addr, split_key, sibling_addr, level + 1, root);
    }
    auto parent_addr = get_path_stack(level + 1);
    assert(parent_addr);
    return internal_page_store(parent_addr, split_key, sibling_addr, root, level + 1);
}

int RemoteBPlusTree::Setup(Target &target) {
    SDS_INFO("Starting B+tree setup");
    uint64_t *root_ptr = (uint64_t *) target.alloc_chunk(1);
    if (!root_ptr) return -1;
    uint32_t *root_raw = (uint32_t *) target.alloc_chunk(1);
    if (!root_raw) return -1;
    assert(kRawPageSize <= kChunkSize);
    LeafPage root;
    int data_off = 0, buf_off = 0;
    uint32_t psn = 1 | LEAF_PAGE_FLAG;
    for (; data_off * sizeof(uint32_t) < kBTreePageSize; data_off += 15, buf_off += 16) {
        root_raw[buf_off] = psn;
        memcpy(root_raw + buf_off + 1, ((uint32_t *) &root) + data_off, 60);
    }
    *root_ptr = target.rel_ptr(root_raw).raw;
    target.set_root_entry(0, target.rel_ptr(root_ptr).raw);
    SDS_INFO("B+tree setup has done");
    return 0;
}

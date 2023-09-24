// Some contents of this file are derived from Sherman 
// https://github.com/thustorage/Sherman

#ifndef SDS_BPLUSTREE_H
#define SDS_BPLUSTREE_H

#include <sys/param.h>

#include "smart/initiator.h"
#include "smart/target.h"

#include "util/inlineskiplist.h"
#include "util/generator.h"
#include "smart/generic_cache.h"

#include <algorithm>
#include <set>
#include <atomic>

using namespace sds;

using rem_addr_t = uint64_t;

const static uint64_t NODEID_MASK = 0xff00000000000000ull;
const static uint64_t OFFSET_MASK = 0x0000ffffffffffffull;
const static uint64_t NODEID_SHIFT = 56;

static inline node_t GetNodeID(rem_addr_t addr) {
    return (addr & NODEID_MASK) >> NODEID_SHIFT;
}

static inline uint64_t GetOffset(rem_addr_t addr) {
    return addr & OFFSET_MASK;
}

static inline rem_addr_t RemoteAddress(node_t node_id, uint64_t offset) {
    return (((uint64_t) node_id) << NODEID_SHIFT) | (offset & OFFSET_MASK);
}

static inline GlobalAddress ToGlobalAddress(rem_addr_t addr, bool dev_mem = false) {
    return {GetNodeID(addr), dev_mem ? DEVICE_MEMORY_MR_ID : MAIN_MEMORY_MR_ID, GetOffset(addr)};
}

class KeyCache {
public:
    void add(uint64_t key, rem_addr_t page_addr, rem_addr_t entry_addr) {
        Entry value;
        value.page_addr = page_addr;
        value.entry_addr = entry_addr;
        cache_.add(key, value);
    }

    bool find(uint64_t key, rem_addr_t &page_addr, rem_addr_t &entry_addr) {
        Entry value;
        if (cache_.find(key, value)) {
            page_addr = value.page_addr;
            entry_addr = value.entry_addr;
            return true;
        } else {
            return false;
        }
    }

    void erase(uint64_t key) {
        cache_.erase(key);
    }

private:
    struct Entry {
        rem_addr_t page_addr;
        rem_addr_t entry_addr;
    };
    GenericCache<uint64_t, Entry> cache_;
};

class IndexCache;

class RemoteBPlusTree {
public:
    const static size_t kValueLength = 8;
    using Key = uint64_t;
    using Value = char[kValueLength];

public:
    static const char *Name() { return "BPlusTree"; }

    static int Setup(Target &target);

    RemoteBPlusTree(JsonConfig config, int max_threads);

    ~RemoteBPlusTree();

    RemoteBPlusTree(const RemoteBPlusTree &) = delete;

    RemoteBPlusTree &operator=(const RemoteBPlusTree &) = delete;

    int search(const std::string &key, std::string &value) {
        return search(to_uint64(key), value);
    }

    int insert(const std::string &key, const std::string &value) {
        return upsert(to_uint64(key), value);
    }

    int update(const std::string &key, const std::string &value) {
        return upsert(to_uint64(key), value);
    }

    int remove(const std::string &key) {
        assert(0 && "unimplemented");
    }

    int scan(const std::string &key, size_t count, std::vector<std::string> &value_list) {
        return scan(to_uint64(key), count, value_list);
    }

    int rmw(const std::string &key, const std::function<std::string(const std::string &)> &transform) {
        return rmw(to_uint64(key), transform);
    }

    std::function<void()> get_poll_task(int &running_tasks) {
        return node_->get_poll_task(running_tasks);
    }

    void run_tasks() {
        node_->run_tasks();
    }

    Initiator *initiator() { return node_; }

public:
    const static size_t kBTreePageSize = 960;
    const static size_t kInternalPageSize = kBTreePageSize;
    const static size_t kLeafPageSize = kBTreePageSize;
    const static size_t kRawPageSize = kBTreePageSize / 60 * 64;
    static_assert(kRawPageSize == 1024, "");

    const static Key kMinKey = 0;
    const static Key kMaxKey = UINT64_MAX;

    struct Header {
        rem_addr_t leftmost;
        rem_addr_t sibling;
        int level;
        int last_index;
        Key min_key;
        Key max_key;

        Header() : leftmost(0), sibling(0), last_index(-1), min_key(kMinKey), max_key(kMaxKey) {}
    }__attribute__((packed));

    struct InternalEntry {
        Key key;
        rem_addr_t ptr;

        InternalEntry() : key(0), ptr(0) {}
    }__attribute__((packed));

    struct LeafEntry {
        Key key;
        Value value;
        char valid_bit;
        char padding[3];
        LeafEntry() : key(0), valid_bit(0) {}
    }__attribute__((packed));

    static_assert(sizeof(InternalEntry) == 16, "");
    // static_assert(sizeof(LeafEntry) == 16, "");
    static_assert(sizeof(LeafEntry) == 20, "");

    constexpr static int kInternalCardinality = (kInternalPageSize - sizeof(Header)) / sizeof(InternalEntry);
    constexpr static int kLeafCardinality = (kLeafPageSize - sizeof(Header)) / sizeof(LeafEntry);

    struct InternalPage {
        Header hdr;
        InternalEntry records[kInternalCardinality];

        InternalPage(rem_addr_t left, const Key &key, rem_addr_t right, int level = 0) {
            hdr.leftmost = left;
            hdr.level = level;
            hdr.last_index = 0;
            records[0].key = key;
            records[0].ptr = right;
        }

        InternalPage(int level = 0) {
            hdr.level = level;
        }

    }__attribute__((packed));

    struct LeafPage {
        Header hdr;
        LeafEntry records[kLeafCardinality];

        LeafPage() {
            hdr.level = 0;
            records[0].valid_bit = 0;
        }
    }__attribute__((packed));

    static_assert(sizeof(Header) % 20 == 0, "");
    static_assert(sizeof(InternalPage) <= kInternalPageSize, "");
    static_assert(sizeof(LeafPage) == kLeafPageSize, "");

    struct LocalLock {
        std::atomic<uint64_t> val;
        bool handover;
        uint8_t handover_depth;

        LocalLock() : val(0), handover_depth(0), handover(false) {}
    };

    struct SearchResult {
        int level;
        rem_addr_t slibing;
        rem_addr_t next_level;
        bool valid_bit;
        Value val;
        rem_addr_t page_addr;
        rem_addr_t entry_addr;
        bool is_leaf() const { return level == 0; }
    };

private:
    uint64_t *get_cas_buf() {
        return cas_buf_ + GetThreadID() * kMaxTasksPerThread + GetTaskID();
    }

    void *get_page_buf(int idx = 0) {
        return page_buf_[idx] + (GetThreadID() * kMaxTasksPerThread + GetTaskID()) * kBTreePageSize;
    }

    uint32_t *get_raw_buf(int idx = 0) {
        return (uint32_t *) (raw_buf_[idx] + (GetThreadID() * kMaxTasksPerThread + GetTaskID()) * kRawPageSize);
    }

private:
    const static size_t kMaxDepth = 12;
    struct PageStack {
        rem_addr_t data[kMaxDepth];
    };

    rem_addr_t get_path_stack(int level) {
        assert(level >= 0 && level < kMaxDepth);
        auto &stack = get_stack();
        return stack.data[level];
    }

    void set_path_stack(int level, rem_addr_t addr) {
        assert(level >= 0 && level < kMaxDepth);
        auto &stack = get_stack();
        stack.data[level] = addr;
    }

    void clear_path_stack() {
        auto &stack = get_stack();
        memset(stack.data, 0, sizeof(PageStack));
    }

    PageStack &get_stack() {
        int idx = GetThreadID() * kMaxTasksPerThread + GetTaskID();
        return stack_[idx];
    }

private:
    int search(const Key &key, std::string &value);

    bool fast_search(const Key &key, std::string &value);

    bool fast_upsert(const Key &key, const std::string &value);

    int upsert(const Key &key, const std::string &value);

    int scan(const Key &key, size_t count, std::vector<std::string> &value_list);

    int rmw(const Key &key, const std::function<std::string(const std::string &)> &transform);

private:
    void HOCL_lock(rem_addr_t lock_addr);

    void HOCL_unlock(rem_addr_t lock_addr);

    rem_addr_t get_lock_addr(rem_addr_t page_addr) {
        uint64_t lock_index = util::FNVHash64(page_addr) % kNumOfLock;
        return RemoteAddress(GetNodeID(page_addr), lock_index * sizeof(uint64_t));
    }

private:
    rem_addr_t get_root_ptr();

    bool update_root_ptr(rem_addr_t old_root_addr, rem_addr_t new_root_addr);

    rem_addr_t alloc_page();

    bool do_page_search(rem_addr_t addr, const Key &key, SearchResult &result, bool cache_hit = false);

    void read_page(rem_addr_t page_addr, void *page_buf, uint32_t *raw_buf);

    void write_page(rem_addr_t page_addr, void *page_buf, uint32_t *raw_buf, bool sync = true);

    void write_leaf_entry(rem_addr_t page_addr, LeafPage *page, LeafEntry *entry);

    void internal_page_search(InternalPage *page, const Key &key, SearchResult &result);

    void leaf_page_search(LeafPage *page, rem_addr_t page_addr, const Key &key, SearchResult &result);

    bool leaf_page_store(rem_addr_t page_addr, const Key &key, const std::string &value,
                         rem_addr_t root_addr, bool cache_hit = false);

    bool leaf_page_rmw(rem_addr_t page_addr, const Key &key,
                       const std::function<std::string(const std::string &)> &transform,
                       rem_addr_t root_addr, bool cache_hit = false);

    bool update_new_root(rem_addr_t left, const Key &key, rem_addr_t right, int level, rem_addr_t old_root_addr);

    bool insert_internal(const Key &key, rem_addr_t value, int level);

    bool internal_page_store(rem_addr_t page_addr, const Key &key, rem_addr_t value, rem_addr_t root, int level);

private:
    static uint64_t to_uint64(const std::string &str) {
        // return *(uint64_t *) str.c_str();
        return atoi(str.c_str());
    }

private:
    JsonConfig config_;
    Initiator *node_;
    rem_addr_t root_ptr_addr_;
    rem_addr_t root_ptr_;
    LocalLock **local_locks_;
    IndexCache *index_cache_;
    uint64_t *cas_buf_;
    char *page_buf_[2];
    char *raw_buf_[2];
    PageStack *stack_;
    int nr_shards_;
    // KeyCache key_cache_[kMaxThreads];
    KeyCache key_cache_;
    bool fast_search_;
};

class IndexCache {
private:
    using Key = RemoteBPlusTree::Key;
    using InternalPage = RemoteBPlusTree::InternalPage;

    struct InternalPageEntry {
        InternalPage data;
        uint64_t ref_cnt;
    };

    struct CacheEntry {
        Key min_key;
        Key max_key;
        mutable InternalPageEntry *page;
    };

    inline static CacheEntry Decode(const char *val) { return *(CacheEntry *) val; }

    struct CacheEntryComparator {
        typedef CacheEntry DecodedType;

        static DecodedType decode_key(const char *b) {
            return Decode(b);
        }

        int cmp(const DecodedType lhs, const DecodedType rhs) const {
            if (lhs.max_key < rhs.max_key) {
                return -1;
            } else if (lhs.max_key > rhs.max_key) {
                return +1;
            } else if (lhs.min_key < rhs.min_key) {
                return +1;
            } else if (lhs.min_key > rhs.min_key) {
                return -1;
            } else {
                return 0;
            }
        }

        int operator()(const char *a, const char *b) const {
            return cmp(Decode(a), Decode(b));
        }

        int operator()(const char *a, const DecodedType b) const {
            return cmp(Decode(a), b);
        }
    };

    using CacheSkipList = InlineSkipList<CacheEntryComparator>;

public:
    IndexCache(int cache_size) : total_pages_(cache_size * kMegaBytes / sizeof(InternalPage)) {
        index_ = new CacheSkipList(comparator_, &allocator_, 21);
        free_pages_.store(total_pages_);
    }

    bool add(InternalPage *page) {
        InternalPageEntry *new_page = new InternalPageEntry();
        memcpy(&new_page->data, page, sizeof(InternalPage));
        new_page->ref_cnt = 0;
        if (add_entry(page->hdr.min_key, page->hdr.max_key, new_page)) {
            if (free_pages_.fetch_sub(1) <= 0) {
                evict_one_page();
            }
            return true;
        } else {
            auto entry = find_entry(page->hdr.min_key, page->hdr.max_key);
            if (entry && entry->min_key == page->hdr.min_key && entry->max_key == page->hdr.max_key) {
                InternalPageEntry *ptr = entry->page;
                if (ptr == nullptr && __sync_bool_compare_and_swap(&entry->page, ptr, new_page)) {
                    if (free_pages_.fetch_sub(1) <= 0) {
                        evict_one_page();
                    }
                    return true;
                }
            }
            delete new_page;
            return false;
        }
    }

    const CacheEntry *lookup(const Key &key, rem_addr_t &addr) {
        auto entry = find_entry(key);
        InternalPageEntry *page_entry = entry ? entry->page : nullptr;
        if (!page_entry || entry->min_key > key || entry->max_key <= key) {
            return nullptr;
        }
        page_entry->ref_cnt++;
        auto page = &page_entry->data;
        if (key < page->records[0].key) {
            addr = page->hdr.leftmost;
        } else {
            for (int i = 1; i <= page->hdr.last_index; ++i) {
                if (key < page->records[i].key) {
                    addr = page->records[i - 1].ptr;
                    return entry;
                }
            }
            addr = page->records[page->hdr.last_index].ptr;
        }
        return entry;
    }

    void invalidate(const CacheEntry *entry) {
        InternalPageEntry *page = entry->page;
        if (!page) return;
        compiler_fence();
        if (__sync_bool_compare_and_swap(&entry->page, page, nullptr)) {
            // delete page;
            free_pages_.fetch_add(1);
        }
    }

private:
    bool add_entry(const Key &min_key, const Key &max_key, InternalPageEntry *ptr) {
        auto buf = index_->AllocateKey(sizeof(CacheEntry));
        auto &entry = *(CacheEntry *) buf;
        entry.min_key = min_key;
        entry.max_key = max_key;
        entry.page = ptr;
        return index_->InsertConcurrently(buf);
    }

    const CacheEntry *find_entry(const Key &key) {
        return find_entry(key, key);
    }

    const CacheEntry *find_entry(const Key &min_key, const Key &max_key) {
        CacheSkipList::Iterator iter(index_);
        CacheEntry entry;
        entry.min_key = min_key;
        entry.max_key = max_key;
        iter.Seek((char *) &entry);
        return iter.Valid() ? (const CacheEntry *) iter.key() : nullptr;
    }

    const CacheEntry *choose_used_entry(InternalPageEntry *&page) {
        uint32_t seed = rdtsc();
        rem_addr_t dummy_addr;
        while (true) {
            auto key = rand_r(&seed) % (100ull * kMegaBytes);
            auto entry = lookup(key, dummy_addr);
            if (!entry) {
                continue;
            }
            auto my_page = entry->page;
            if (my_page) {
                continue;
            }
            page = my_page;
            return entry;
        }
    }

    void evict_one_page() {
        uint64_t min_ref_cnt = UINT64_MAX;
        InternalPageEntry *target_page;
        const CacheEntry *target_entry;
        for (int i = 0; i < 2; ++i) {
            InternalPageEntry *page;
            auto entry = choose_used_entry(page);
            if (page->ref_cnt < min_ref_cnt) {
                min_ref_cnt = page->ref_cnt;
                target_entry = entry;
                target_page = page;
            }
        }
        if (__sync_bool_compare_and_swap(&target_entry->page, target_page, nullptr)) {
            // free(ptr);
            free_pages_.fetch_add(1);
        }
    }

private:
    CacheSkipList *index_;
    const int64_t total_pages_;
    std::atomic<int64_t> free_pages_;
    CacheEntryComparator comparator_;
    Allocator allocator_;
};

#endif //SDS_BPLUSTREE_H

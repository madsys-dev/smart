// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#ifndef SDS_MEMSTORE_H
#define SDS_MEMSTORE_H

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <cassert>

#include "common.h"
#include "util/murmur.h"

#define OFFSET_NOT_FOUND -1
#define OFFSET_FOUND 0
#define VERSION_TOO_OLD -2  // The new version < old version

#define SLOT_NOT_FOUND -1
#define SLOT_FOUND 0

struct DataItem {
    table_id_t table_id;
    size_t value_size;
    itemkey_t key;
    offset_t remote_offset;
    version_t version;
    lock_t lock;
    uint8_t value[MAX_ITEM_SIZE];
    uint8_t valid;        // 1: Not deleted, 0: Deleted
    uint8_t user_insert;  // 1: User insert operation, 0: Not user insert operation

    DataItem() {}

    // Build an empty item for fetching data from remote
    DataItem(table_id_t t, itemkey_t k)
            : table_id(t), value_size(0), key(k), remote_offset(0), version(0), lock(0), valid(1), user_insert(0) {}

    // For user insert item
    DataItem(table_id_t t, size_t s, itemkey_t k, version_t v, uint8_t ins)
            : table_id(t), value_size(s), key(k), remote_offset(0), version(v), lock(0), valid(1), user_insert(ins) {}

    // For server load data
    DataItem(table_id_t t, size_t s, itemkey_t k, uint8_t *d)
            : table_id(t), value_size(s), key(k), remote_offset(0), version(0), lock(0), valid(1), user_insert(0) {
        memcpy(value, d, s);
    }

    ALWAYS_INLINE size_t GetSerializeSize() const {
        return sizeof(*this);
    }

    ALWAYS_INLINE void Serialize(char *undo_buffer) {
        memcpy(undo_buffer, (char *) this, sizeof(*this));
    }

    ALWAYS_INLINE uint64_t GetRemoteLockAddr() {
        return remote_offset + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset) +
               sizeof(version);
    }

    ALWAYS_INLINE uint64_t GetRemoteLockAddr(offset_t remote_item_off) {
        return remote_item_off + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset) +
               sizeof(version);
    }

    ALWAYS_INLINE uint64_t GetRemoteVersionAddr() {
        return remote_offset + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset);
    }

    ALWAYS_INLINE uint64_t GetRemoteVersionAddr(offset_t remote_item_off) {
        return remote_item_off + sizeof(table_id) + sizeof(value_size) + sizeof(key) + sizeof(remote_offset);
    }

} Aligned8;  // Size: 560B in X86 arch.

const static size_t DataItemSize = sizeof(DataItem);
const static size_t RFlushReadSize = 1;  // The size of RDMA read, that is after write to emulate rdma flush
using DataItemPtr = std::shared_ptr<DataItem>;
const int ITEM_NUM_PER_NODE = 22;

struct MemStoreAllocParam {
    // The start of the registered memory region for storing memory stores
    char *mem_region_start;
    // The start of the whole memory store space (e.g., Hash Store Space)
    char *mem_store_start;
    // The start offset of each memory store instance
    offset_t mem_store_alloc_offset;
    // The start address of the whole reserved space (e.g., for insert in hash conflict). Here for overflow check
    char *mem_store_reserve;

    MemStoreAllocParam(char *region_start, char *store_start, offset_t start_off, char *reserve_start)
            : mem_region_start(region_start), mem_store_start(store_start),
              mem_store_alloc_offset(start_off), mem_store_reserve(reserve_start) {}
};

struct MemStoreReserveParam {
    // The start address of the whole reserved space (e.g., for insert in hash conflict).
    char *mem_store_reserve;
    // For allocation in case of memory store (e.g., HashStore) conflict
    offset_t mem_store_reserve_offset;
    // The end address of the memory store space. Here for overflow check
    char *mem_store_end;

    MemStoreReserveParam(char *reserve_start, offset_t reserve_off, char *end)
            : mem_store_reserve(reserve_start), mem_store_reserve_offset(reserve_off), mem_store_end(end) {}
};

struct HashMeta {
    // To which table this hash store belongs
    table_id_t table_id;

    // Virtual address of the table, used to calculate the distance
    // between some HashNodes with the table for traversing
    // the linked list
    uint64_t data_ptr;

    // Offset of the table, relative to the RDMA local_mr
    offset_t base_off;

    // Total hash buckets
    uint64_t bucket_num;

    // Size of hash node
    size_t node_size;

    HashMeta(table_id_t table_id, uint64_t data_ptr, uint64_t bucket_num, size_t node_size, offset_t base_off)
            : table_id(table_id), data_ptr(data_ptr), base_off(base_off), bucket_num(bucket_num),
              node_size(node_size) {}

    HashMeta() {}
} Aligned8;

class HashStore {
public:
    struct HashNode {
        DataItem data_items[ITEM_NUM_PER_NODE];
        HashNode *next;
    } Aligned8;

    HashStore(table_id_t table_id, uint64_t bucket_num, MemStoreAllocParam *param)
            : table_id(table_id), base_off(0), bucket_num(bucket_num), data_ptr(nullptr), node_num(0) {
        assert(bucket_num > 0);
        table_size = (bucket_num) * sizeof(HashNode);
        region_start_ptr = param->mem_region_start;
        assert((uint64_t) param->mem_store_start + param->mem_store_alloc_offset + table_size <=
               (uint64_t) param->mem_store_reserve);
        data_ptr = param->mem_store_start + param->mem_store_alloc_offset;
        param->mem_store_alloc_offset += table_size;

        base_off = (uint64_t) data_ptr - (uint64_t) region_start_ptr;
        assert(base_off >= 0);
        assert(data_ptr != nullptr);
        memset(data_ptr, 0, table_size);
    }

    table_id_t GetTableID() const {
        return table_id;
    }

    offset_t GetBaseOff() const {
        return base_off;
    }

    uint64_t GetHashNodeSize() const {
        return sizeof(HashNode);
    }

    uint64_t GetBucketNum() const {
        return bucket_num;
    }

    char *GetDataPtr() const {
        return data_ptr;
    }

    offset_t GetItemRemoteOffset(const void *item_ptr) const {
        return (uint64_t) item_ptr - (uint64_t) region_start_ptr;
    }

    uint64_t TableSize() const {
        return table_size;
    }

    uint64_t GetHash(itemkey_t key) {
        return MurmurHash64A(key, 0xdeadbeef) % bucket_num;
    }

    DataItem *LocalGet(itemkey_t key);

    DataItem *LocalInsert(itemkey_t key, const DataItem &data_item, MemStoreReserveParam *param);

    DataItem *LocalPut(itemkey_t key, const DataItem &data_item, MemStoreReserveParam *param);

    bool LocalDelete(itemkey_t key);

private:
    table_id_t table_id;
    offset_t base_off;
    uint64_t bucket_num;
    char *data_ptr;
    uint64_t node_num;
    size_t table_size;
    char *region_start_ptr;
};

ALWAYS_INLINE
DataItem *HashStore::LocalGet(itemkey_t key) {
    uint64_t hash = GetHash(key);
    auto *node = (HashNode *) (hash * sizeof(HashNode) + data_ptr);
    while (node) {
        for (auto &data_item: node->data_items) {
            if (data_item.valid && data_item.key == key) {
                return &data_item;
            }
        }
        node = node->next;
    }
    return nullptr;  // failed to found one
}

ALWAYS_INLINE
DataItem *HashStore::LocalInsert(itemkey_t key, const DataItem &data_item, MemStoreReserveParam *param) {
    uint64_t hash = GetHash(key);
    auto *node = (HashNode *) (hash * sizeof(HashNode) + data_ptr);

    while (node) {
        for (auto &item: node->data_items) {
            if (!item.valid) {
                item = data_item;
                item.valid = 1;
                return &item;
            }
        }
        if (!node->next) break;
        node = node->next;
    }

    assert((uint64_t) param->mem_store_reserve + param->mem_store_reserve_offset <=
           (uint64_t) param->mem_store_end);
    auto *new_node = (HashNode *) (param->mem_store_reserve + param->mem_store_reserve_offset);
    param->mem_store_reserve_offset += sizeof(HashNode);
    memset(new_node, 0, sizeof(HashNode));
    new_node->data_items[0] = data_item;
    new_node->data_items[0].valid = 1;
    new_node->next = nullptr;
    node->next = new_node;
    node_num++;
    return &(new_node->data_items[0]);
}

ALWAYS_INLINE
DataItem *HashStore::LocalPut(itemkey_t key, const DataItem &data_item, MemStoreReserveParam *param) {
    DataItem *res;
    if ((res = LocalGet(key)) != nullptr) {
        *res = data_item;
        return res;
    }
    return LocalInsert(key, data_item, param);
}

ALWAYS_INLINE
bool HashStore::LocalDelete(itemkey_t key) {
    uint64_t hash = GetHash(key);
    auto *node = (HashNode *) (hash * sizeof(HashNode) + data_ptr);
    for (auto &data_item: node->data_items) {
        if (data_item.valid && data_item.key == key) {
            data_item.valid = 0;
            return true;
        }
    }
    node = node->next;
    while (node) {
        for (auto &data_item: node->data_items) {
            if (data_item.valid && data_item.key == key) {
                data_item.valid = 0;
                return true;
            }
        }
        node = node->next;
    }
    return false;
}

#endif //SDS_MEMSTORE_H

// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#pragma once

#include <cstddef>  // For size_t
#include <cstdint>  // For uintxx_t

#include "smart/common.h"

// Global specification
using tx_id_t = uint64_t;     // Transaction id type
using t_id_t = uint32_t;      // Thread id type
using coro_id_t = int;        // Coroutine id type
using node_id_t = int;        // Machine id type
using table_id_t = uint64_t;  // Table id type
using itemkey_t = uint64_t;   // Data item key type, used in DB tables
using offset_t = int64_t;     // Offset type. Usually used in remote offset for RDMA
using version_t = uint64_t;   // Version type, used in version checking
using lock_t = uint64_t;      // Lock type, used in remote locking

// Max data item size.
// 8: smallbank
// 40: tatp
// 664: tpcc
#ifdef CONFIG_ITEM_SIZE
const size_t MAX_ITEM_SIZE = (CONFIG_ITEM_SIZE);
#else
const size_t MAX_ITEM_SIZE = 8;
#endif

#define BACKUP_DEGREE 1                     // Backup memory node number. MUST **NOT** BE SET TO 0

// Data state
#define STATE_INVISIBLE 0x8000000000000000  // Data cannot be read
#define STATE_LOCKED 1                      // Data cannot be written. Used for serializing transactions
#define STATE_CLEAN 0

// Alias
#define Aligned8 __attribute__((aligned(8)))
#define ALWAYS_INLINE inline __attribute__((always_inline))

// Helpful for improving condition prediction hit rate
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)

const offset_t LOG_BUFFER_SIZE = 1024 * 1024 * 1024;
const node_id_t NUM_MEMORY_NODES = BACKUP_DEGREE + 1;

// Remote offset to write log
class LogOffsetAllocator {
public:
    LogOffsetAllocator(t_id_t tid, t_id_t num_thread) {
        auto per_thread_remote_log_buffer_size = LOG_BUFFER_SIZE / num_thread;
        for (node_id_t i = 0; i < NUM_MEMORY_NODES; i++) {
            start_log_offsets[i] = tid * per_thread_remote_log_buffer_size;
            end_log_offsets[i] = (tid + 1) * per_thread_remote_log_buffer_size;
            current_log_offsets[i] = 0;
        }
    }

    offset_t GetNextLogOffset(node_id_t node_id, size_t log_entry_size) {
        if (unlikely(start_log_offsets[node_id] + current_log_offsets[node_id] + log_entry_size >
                     end_log_offsets[node_id])) {
            current_log_offsets[node_id] = 0;
        }
        offset_t offset = start_log_offsets[node_id] + current_log_offsets[node_id];
        current_log_offsets[node_id] += log_entry_size;
        return offset;
    }

private:
    offset_t start_log_offsets[NUM_MEMORY_NODES];
    offset_t end_log_offsets[NUM_MEMORY_NODES];
    offset_t current_log_offsets[NUM_MEMORY_NODES];
};


// Alloc registered RDMA buffer for each thread
class RDMABufferAllocator {
public:
    RDMABufferAllocator(char *s, char *e) : start(s), end(e), cur_offset(0) {}

    char *Alloc(size_t size) {
        if (unlikely(start + cur_offset + size > end)) {
            cur_offset = 0;
        }
        char *ret = start + cur_offset;
        cur_offset += size;
        return ret;
    }

private:
    char *start;
    char *end;
    uint64_t cur_offset;
};

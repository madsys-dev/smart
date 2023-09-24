/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2022-2023 Feng Ren, Tsinghua University 
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

#ifndef SDS_THREAD_H
#define SDS_THREAD_H

#include <atomic>
#include <cstddef>
#include <cassert>
#include <numa.h>
#include <map>
#include <pthread.h>

#include "common.h"

namespace sds {
    extern void thread_registry_deregister_thread(int tid);

    struct ThreadCheckInCheckOut {
        static const int kThreadNotAssigned = -1;

        int tid{kThreadNotAssigned};
        int padding[15];

        ~ThreadCheckInCheckOut() {
            if (tid == kThreadNotAssigned) return;
            thread_registry_deregister_thread(tid);
        }
    };

    extern thread_local ThreadCheckInCheckOut tl_tcico;

    class ThreadRegistry;

    extern ThreadRegistry gThreadRegistry;

    extern int gBindToCore[kMaxThreads];
    extern int gBindToSocket[kMaxThreads];

    class ThreadRegistry {
    private:
        alignas(128) std::atomic<bool> usedTid[kMaxThreads];
        alignas(128) std::atomic<int> maxTid{-1};

    public:
        ThreadRegistry() {
            for (int it = 0; it < kMaxThreads; it++) {
                usedTid[it].store(false, std::memory_order_relaxed);
            }
        }

        int register_thread_new() {
            for (int tid = 0; tid < kMaxThreads; tid++) {
                if (usedTid[tid].load(std::memory_order_acquire)) continue;
                bool unused = false;
                if (!usedTid[tid].compare_exchange_strong(unused, true)) continue;
                int curMax = maxTid.load();
                while (curMax <= tid) {
                    maxTid.compare_exchange_strong(curMax, tid + 1);
                    curMax = maxTid.load();
                }
                tl_tcico.tid = tid;
                return tid;
            }
            SDS_WARN("unreachable");
            exit(EXIT_FAILURE);
        }

        inline void deregister_thread(const int tid) {
            usedTid[tid].store(false, std::memory_order_release);
        }

        static inline int GetMaxThreads() {
            return gThreadRegistry.maxTid.load(std::memory_order_acquire);
        }

        static inline int GetTID() {
            int tid = tl_tcico.tid;
            if (tid != ThreadCheckInCheckOut::kThreadNotAssigned) return tid;
            return gThreadRegistry.register_thread_new();
        }

        static inline int GetSocketID() {
            return gBindToSocket[GetTID()];
        }
    };

    static inline int GetThreadID() {
        return ThreadRegistry::GetTID();
    }

    static inline void BindCore(int core_id) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(core_id, &set);
        pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
        int socket_id = numa_node_of_cpu(core_id % numa_num_configured_cpus());
        assert(socket_id >= 0);
        // numa_set_localalloc();
        gBindToCore[GetThreadID()] = core_id;
        gBindToSocket[GetThreadID()] = socket_id;
    }

    static inline void RemapBindCore(int id) {
        int cpus = numa_num_configured_cpus();
        int sockets = numa_num_configured_nodes();
        assert(sockets);
        int cpus_per_socket = cpus / sockets;
        int core_id = (id % cpus_per_socket) * sockets + (id / cpus_per_socket);
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(core_id, &set);
        pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
        int socket_id = numa_node_of_cpu(core_id % numa_num_configured_cpus());
        assert(socket_id >= 0);
        gBindToCore[GetThreadID()] = core_id;
        gBindToSocket[GetThreadID()] = socket_id;
    }

    static inline void WritePidFile() {
        FILE *fin = fopen("/tmp/sds.pid", "w");
        assert(fin);
        int rc = fprintf(fin, "%d", getpid());
        assert(rc > 0);
        rc = fclose(fin);
        assert(rc == 0);
    }

    struct StatInfo {
        uint64_t retry_count = 0;
        uint64_t retry_sum = 0;
        uint64_t read_bytes = 0;
        uint64_t total_ops = 0;
        uint64_t cache_hit = 0;
        std::map<int, int> hist;
    };

    extern StatInfo tl_stat[kMaxThreads];

// #define CONFIG_STAT
#ifdef CONFIG_STAT
    static inline void RecordRetry() {
        tl_stat[GetThreadID()].retry_count++;
        tl_stat[GetThreadID()].retry_sum++;
    }

    static inline void RecordCacheHit() {
        tl_stat[GetThreadID()].cache_hit++;
    }

    static inline void RecordOps() {
        auto &entry = tl_stat[GetThreadID()];
        if (entry.retry_count >= 8) entry.retry_count = 8;
        entry.hist[entry.retry_count]++;
        entry.retry_count = 0;
        entry.total_ops++;
    }

    static inline void RecordReadBytes(size_t bytes) {
        tl_stat[GetThreadID()].read_bytes += bytes;
    }

    static inline void ResetStat() {
        auto &entry = tl_stat[GetThreadID()];
        entry.retry_count = 0;
        entry.retry_sum = 0;
        entry.read_bytes = 0;
        entry.total_ops = 0;
        entry.cache_hit = 0;
        entry.hist.clear();
    }

    static inline StatInfo AggregateStat() {
        StatInfo ret;
        for (int i = 0; i < kMaxThreads; ++i) {
            ret.read_bytes += tl_stat[i].read_bytes;
            ret.total_ops += tl_stat[i].total_ops;
            ret.cache_hit += tl_stat[i].cache_hit;
            ret.retry_sum += tl_stat[i].retry_sum;
            for (auto &e: tl_stat[i].hist) {
                ret.hist[e.first] += e.second;
            }
        }
        return ret;
    }
#else
    static inline void RecordRetry() {}

    static inline void RecordCacheHit() {}

    static inline void RecordOps() {}

    static inline void RecordReadBytes(size_t bytes) {}

    static inline void ResetStat() {}

    static inline StatInfo AggregateStat() {
        StatInfo ret;
        return ret;
    }
#endif
}

#endif //SDS_THREAD_H

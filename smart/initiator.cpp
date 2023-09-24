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

#include <unistd.h>

#include "smart/initiator.h"

namespace sds {
    Initiator::Initiator() : tuner_running_(false), cache_offset_(0) {
        size_t size_idx = 0;
        for (int id = 0; id < kNumFreeLists; ++id) {
            while (size_idx <= kPowerOfTwo[id]) {
                kSizeToClass[size_idx] = id;
                ++size_idx;
            }
        }

        auto &config = manager_.config();
        cache_size_ = config.initiator_cache_size;
        cache_ = mmap_huge_page(cache_size_);
        if (manager_.register_main_memory(cache_, cache_size_, MR_FULL_PERMISSION)) {
            exit(EXIT_FAILURE);
        }

        for (auto &tl: tl_data_) {
            tl.qp_state.post_req.resize(config.max_nodes, 0);
            tl.qp_state.ack_req.resize(config.max_nodes, 0);
            tl.credit = config.initial_credit;
            tl.ack_req_estimation = 0;
            tl.task_lock = false;
        }

        if (config.throttler && config.throttler_auto_tuning) {
            tuner_running_ = true;
            tuner_ = std::thread(&Initiator::tuner_func, this);
        }
    }

    Initiator::Initiator(void *cache_pptr) : tuner_running_(false), cache_offset_(0) {
        assert(cache_pptr);
        size_t size_idx = 0;
        for (int id = 0; id < kNumFreeLists; ++id) {
            while (size_idx <= kPowerOfTwo[id]) {
                kSizeToClass[size_idx] = id;
                ++size_idx;
            }
        }

        auto &config = manager_.config();
        cache_size_ = config.initiator_cache_size;
        cache_ = cache_pptr;
        if (manager_.register_main_memory(cache_, cache_size_, MR_FULL_PERMISSION)) {
            exit(EXIT_FAILURE);
        }

        for (auto &tl: tl_data_) {
            tl.qp_state.post_req.resize(config.max_nodes, 0);
            tl.qp_state.ack_req.resize(config.max_nodes, 0);
            tl.credit = config.initial_credit;
            tl.ack_req_estimation = 0;
            tl.task_lock = false;
        }

        if (config.throttler && config.throttler_auto_tuning) {
            tuner_running_ = true;
            tuner_ = std::thread(&Initiator::tuner_func, this);
        }
    }

    Initiator::~Initiator() {
        if (tuner_running_) {
            tuner_running_ = false;
            tuner_.join();
        }
    }

    GlobalAddress Initiator::alloc_memory(node_t mem_node_id, size_t size) {
        auto &free_list = tl_data_[GetThreadID()].free_list[mem_node_id];
        if (size >= kChunkSize) {
            return do_alloc_chunk(mem_node_id, (size + kChunkSize - 1) / kChunkSize);
        }
        int sz_id = kSizeToClass[size];
        if (!free_list.sz[sz_id].empty()) {
            return free_list.sz[sz_id].dequeue();
        } else {
            return do_alloc_memory(mem_node_id, sz_id);
        }
    }

    GlobalAddress Initiator::do_alloc_memory(node_t mem_node_id, int sz_id) {
        auto &free_list = tl_data_[GetThreadID()].free_list[mem_node_id];
        GlobalAddress offset;
        if (sz_id == kNumFreeLists - 1) {
            if (free_list.sz[sz_id].empty()) {
                offset = do_alloc_chunk(mem_node_id, 1);
                if (offset == NULL_GLOBAL_ADDRESS) {
                    return offset;
                }
                for (size_t i = kPowerOfTwo[sz_id]; i < kChunkSize; i += kPowerOfTwo[sz_id]) {
                    free_list.sz[sz_id].enqueue(offset.raw + i);
                }
                return offset;
            } else {
                return free_list.sz[sz_id].dequeue();
            }
        }
        if (!free_list.sz[sz_id].empty()) {
            return free_list.sz[sz_id].dequeue();
        } else {
            offset = do_alloc_memory(mem_node_id, sz_id + 1);
            if (offset == NULL_GLOBAL_ADDRESS) {
                return offset;
            }
            free_list.sz[sz_id].enqueue(offset.raw + kPowerOfTwo[sz_id]);
            return offset;
        }
    }

    int Initiator::free_memory(GlobalAddress addr, size_t size) {
        assert(addr.node <= manager_.max_node_id());
        auto &free_list = tl_data_[GetThreadID()].free_list[addr.node];
        if (size >= kChunkSize) {
            int id = kNumFreeLists - 1;
            for (size_t i = 0; i < size; i += kChunkSize) {
                free_list.sz[id].enqueue(addr.raw + i);
            }
        } else {
            int id = kSizeToClass[size];
            free_list.sz[id].enqueue(addr.raw);
        }
        return 0;
    }

    void *Initiator::alloc_cache(size_t size) {
        if (size % kCacheLineSize) {
            size = size - (size % kCacheLineSize) + kCacheLineSize;
        }
        uint64_t offset = cache_offset_.fetch_add(size);
        if (offset + size <= cache_size_) {
            return (char *) cache_ + offset;
        } else {
            cache_offset_.fetch_sub(size);
            return nullptr;
        }
    }

    SuperChunk *Initiator::find_super_chunk(node_t mem_node_id) {
        auto &tl = tl_data_[GetThreadID()];
        if (!tl.super_chunk.count(mem_node_id)) {
            tl.super_chunk[mem_node_id] = (SuperChunk *) alloc_cache(sizeof(SuperChunk));
            assert(tl.super_chunk[mem_node_id]);
            int rc = read(tl.super_chunk[mem_node_id],
                          GlobalAddress(mem_node_id, MAIN_MEMORY_MR_ID, 0),
                          sizeof(SuperChunk), Option::Sync);
            assert(!rc);
        }
        return tl.super_chunk[mem_node_id];
    }

    GlobalAddress Initiator::do_alloc_chunk(node_t mem_node_id, int count) {
        auto &tl = tl_data_[GetThreadID()];
        int rc;
        TaskLockGuard lock(tl.task_lock);
        auto entry = find_super_chunk(mem_node_id);
        while (true) {
            if (entry->alloc_chunk + count <= entry->max_chunk) {
                uint64_t old_alloc_chunk = entry->alloc_chunk;
                auto remote_addr = GlobalAddress(mem_node_id, MAIN_MEMORY_MR_ID,
                                                 offsetof(SuperChunk, alloc_chunk));
                rc = compare_and_swap(&entry->alloc_chunk, remote_addr, old_alloc_chunk,
                                      old_alloc_chunk + count, Option::Sync);
                assert(!rc);
                if (entry->alloc_chunk == old_alloc_chunk) {
                    return {mem_node_id, MAIN_MEMORY_MR_ID, entry->alloc_chunk * kChunkSize};
                }
            } else {
                return NULL_GLOBAL_ADDRESS;
            }
        }
    }

    int Initiator::set_root_entry(node_t mem_node_id, uint8_t index, uint64_t value) {
        auto &tl = tl_data_[GetThreadID()];
        int rc;
        TaskLockGuard lock(tl.task_lock);
        auto super = find_super_chunk(mem_node_id);
        auto entry = &super->root_entries[index];
        auto remote_addr = GlobalAddress(mem_node_id, (uint64_t) entry - (uint64_t) super);
        *entry = value;
        rc = write(entry, remote_addr, sizeof(uint64_t), Option::Sync);
        assert(!rc);
        return 0;
    }

    int Initiator::get_root_entry(node_t mem_node_id, uint8_t index, uint64_t &value) {
        auto &tl = tl_data_[GetThreadID()];
        int rc;
        TaskLockGuard lock(tl.task_lock);
        auto super = find_super_chunk(mem_node_id);
        auto entry = &super->root_entries[index];
        auto remote_addr = GlobalAddress(mem_node_id, (uint64_t) entry - (uint64_t) super);
        rc = read(entry, remote_addr, sizeof(uint64_t), Option::Sync);
        assert(!rc);
        value = *entry;
        return 0;
    }

    int Initiator::add_request(ibv_wr_opcode opcode, const void *local, const GlobalAddress &remote, size_t length,
                               uint64_t compare_add, uint64_t swap, int flags) {
        auto &req_buf = tl_data_[GetThreadID()].req_buf[GetTaskID()];
        assert(remote.node < manager_.config().max_nodes);
        assert((uint64_t) local >= (uint64_t) cache_ && (uint64_t) local < (uint64_t) cache_ + cache_size_);
        uint32_t rkey;
        uintptr_t remote_va;
        if (manager_.get_remote_memory_key(remote.node, remote.mr_id, remote.offset, length, rkey, remote_va)) {
            return -1;
        }

        if (req_buf.size && req_buf.mem_node_id != remote.node) {
            if (sync()) {
                return -1;
            }
            assert(req_buf.size == 0);
        }

        int idx = req_buf.size;
        if (req_buf.size + 1 == RequestBuffer::kCapacity) {
            if (sync()) {
                return -1;
            }
            assert(req_buf.size + 1 < RequestBuffer::kCapacity);
            idx = req_buf.size;
        }

        req_buf.size++;
        req_buf.mem_node_id = remote.node;

        auto &wr = req_buf.wr_list[idx];
        auto &sge = req_buf.sge_list[idx];
        sge.length = length;
        sge.lkey = manager_.get_local_memory_lkey(0);
        sge.addr = (uintptr_t) local;
        wr.wr_id = 0;
        wr.opcode = opcode;
        wr.num_sge = 1;
        wr.sg_list = &sge;
        wr.send_flags = flags;
        wr.next = nullptr;
        if (opcode == IBV_WR_ATOMIC_FETCH_AND_ADD || opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
            wr.wr.atomic.rkey = rkey;
            wr.wr.atomic.remote_addr = remote_va + remote.offset;
            wr.wr.atomic.compare_add = compare_add;
            wr.wr.atomic.swap = swap;
        } else {
            wr.wr.rdma.rkey = rkey;
            wr.wr.rdma.remote_addr = remote_va + remote.offset;
        }
        if (opcode == IBV_WR_RDMA_WRITE && length <= manager_.config().max_inline_data) {
            wr.send_flags |= IBV_SEND_INLINE;
        }
        if (idx > 0) {
            req_buf.wr_list[idx - 1].next = &wr;
        }
        return 0;
    }

    static inline uint64_t MAKE_WR_ID(node_t mem_node_id, int wr_size) {
        int tid = GetThreadID();
        int task_id = GetTaskID();
        return  ((uint64_t) task_id << 56ull) + ((uint64_t) tid << 48ull) + ((uint64_t) mem_node_id << 32ull) + wr_size;
    }

    static inline node_t MEM_NODE_ID(uint64_t wr_id) {
        return (node_t) ((wr_id >> 32ull) & UINT16_MAX);
    }

    static inline int THREAD_ID(uint64_t wr_id) {
        return (int) (wr_id >> 48ull) & UINT8_MAX;
    }

    static inline int TASK_ID(uint64_t wr_id) {
        return (int) (wr_id >> 56ull) & UINT8_MAX;
    }

    static inline int WR_SIZE(uint64_t wr_id) {
        return (int) (wr_id & UINT32_MAX);
    }

    int Initiator::post_request() {
        auto &tl = tl_data_[GetThreadID()];
        auto &req_buf = tl.req_buf[GetTaskID()];
        auto &state = tl.qp_state;
        if (!req_buf.size) {
            return 0;
        }
        int wr_size = req_buf.size;
        if (!manager_.config().qp_sharing && manager_.config().throttler) {
            decrease_credit(req_buf.size);
        }
        req_buf.wr_list[req_buf.size - 1].wr_id = MAKE_WR_ID(req_buf.mem_node_id, wr_size);
        req_buf.wr_list[req_buf.size - 1].send_flags |= IBV_SEND_SIGNALED;
        int qp_idx = GetThreadID() % manager_.get_qp_size(req_buf.mem_node_id);
        if (manager_.post_send(req_buf.mem_node_id, qp_idx, req_buf.wr_list)) {
            return -1;
        }
        req_buf.size = 0;
        state.post_req[req_buf.mem_node_id] += wr_size;
        state.inflight_ack += wr_size;
        return 0;
    }

    std::function<void()> Initiator::get_poll_task(int &running_tasks) {
        return [this, &running_tasks]() {
            bool shared_cq = manager_.config().shared_cq;
            while (running_tasks) {
                int total_ack = 0;
                if (shared_cq) {
                    int rc = poll_once(0, true);
                    if (__glibc_unlikely(rc < 0)) {
                        exit(EXIT_FAILURE);
                    }
                    total_ack += rc;
                } else {
                    for (int id = 0; id <= manager_.max_node_id(); ++id) {
                        int rc = poll_once(id, true);
                        if (__glibc_unlikely(rc < 0)) {
                            exit(EXIT_FAILURE);
                        }
                        total_ack += rc;
                    }
                }
                YieldTask();
            }
        };
    }

    void Initiator::run_tasks() {
        auto &task_pool = TaskPool::Get();
        bool shared_cq = manager_.config().shared_cq;
        while (!task_pool.empty()) {
            if (shared_cq) {
                int rc = poll_once(0, true);
                if (__glibc_unlikely(rc < 0)) {
                    exit(EXIT_FAILURE);
                }
            } else {
                for (int id = 0; id <= manager_.max_node_id(); ++id) {
                    int rc = poll_once(id, true);
                    if (__glibc_unlikely(rc < 0)) {
                        exit(EXIT_FAILURE);
                    }
                }
            }
            YieldTask();
        }
    }

    int Initiator::sync() {
        auto &tl = tl_data_[GetThreadID()];
        auto &state = tl.qp_state;
        if (post_request()) {
            return -1;
        }
        auto &post_req = tl.post_req_snapshot[GetTaskID()];
        post_req.resize(state.post_req.size());
        for (int id = 0; id < post_req.size(); ++id) {
            post_req[id] = state.post_req[id];
        }
        for (int id = 0; id < post_req.size(); ++id) {
            while (state.ack_req[id] < post_req[id]) {
                if (TaskPool::IsEnabled()) {
                    WaitTask();
                } else {
                    if (poll_once(id) < 0) {
                        return -1;
                    }
                }
            }
        }
        return 0;
    }

    int Initiator::poll_once(node_t mem_node_id, bool notify) {
        auto &tl = tl_data_[GetThreadID()];
        auto &state = tl.qp_state;
        int total_ack = 0;
        if (!state.inflight_ack) {
            return 0;
        }
        std::vector<uint64_t> wr_id_list;
        int qp_idx = GetThreadID() % manager_.get_qp_size(mem_node_id);
        int rc = manager_.do_poll(mem_node_id, qp_idx, wr_id_list);
        if (rc < 0) {
            return -1;
        }
        if (manager_.config().qp_sharing) {
            for (auto &wr_id: wr_id_list) {
                int size = WR_SIZE(wr_id);
                int tid = THREAD_ID(wr_id);
                int node_id = MEM_NODE_ID(wr_id);
                __sync_fetch_and_add(&tl_data_[tid].qp_state.ack_req[node_id], size);
                total_ack += size;
                __sync_fetch_and_sub(&tl_data_[tid].qp_state.inflight_ack, size);
                if (notify) {
                   auto &post_req = tl.post_req_snapshot[TASK_ID(wr_id)];
                   if (!post_req.empty() && state.ack_req[node_id] >= post_req[node_id]) {
                       NotifyTask(TASK_ID(wr_id));
                   }
                }
            }
        } else {
            for (auto &wr_id: wr_id_list) {
                int size = WR_SIZE(wr_id);
                int node_id = MEM_NODE_ID(wr_id);
                state.ack_req[MEM_NODE_ID(wr_id)] += size;
                total_ack += size;
                state.inflight_ack -= size;
                if (notify) {
                    auto &post_req = tl.post_req_snapshot[TASK_ID(wr_id)];
                    if (!post_req.empty() && state.ack_req[node_id] >= post_req[node_id]) {
                        NotifyTask(TASK_ID(wr_id));
                    }
                }
            }
            if (total_ack && manager_.config().throttler) {
                increase_credit(total_ack);
            }
        }
        return total_ack;
    }

    void Initiator::decrease_credit(int count) {
        auto &tl = tl_data_[GetThreadID()];
        while (tl.credit.load(std::memory_order_relaxed) < count) {
            if (TaskPool::IsEnabled()) {
                WaitTask();
            } else {
                if (manager_.config().shared_cq) {
                    poll_once(0);
                } else {
                    for (int id = 0; id <= manager_.max_node_id(); ++id) {
                        poll_once(id);
                    }
                }
            }
        }
        tl.credit.fetch_sub(count, std::memory_order_relaxed);
    }

    void Initiator::increase_credit(int count) {
        auto &tl = tl_data_[GetThreadID()];
        int credit = tl.credit.fetch_add(count, std::memory_order_relaxed);
        tl.ack_req_estimation += count;
        if (credit >= 0) {
            NotifyTask();
        }
    }

    void Initiator::tuner_func() {
        const static int kCreditStep = config().credit_step;
        const static int kMaxCreditValue = config().max_credit;
        const static int kInfCreditValue = 256;
        const static int kExecutionEpochs = config().execution_epochs;
        const static uint64_t kSamplingCycles = config().sample_cycles;
        const double kInfCreditWeight = config().inf_credit_weight;
        BindCore(0);

        uint64_t epoch_clock = rdtsc();
        int credit_bound = manager_.config().initial_credit;
        bool is_training = true;
        int exec_epochs = 0;

        uint64_t best_ack_req = 0;
        int best_max_credit = -1;
        usleep(12000);
        uint64_t ack_req_list[kInfCreditValue] = {0};
        while (tuner_running_) {
            int credit_delta = 0;
            uint64_t curr_clock = rdtsc();
            while (curr_clock - epoch_clock < kSamplingCycles) {
                usleep(1000);
                curr_clock = rdtsc();
            }
            double factor = (curr_clock - epoch_clock) * 1.0 / kSamplingCycles;
            epoch_clock = curr_clock;
            uint64_t ack_req_sum = 0;
            for (int i = 0; i < kMaxThreads; ++i) {
                ack_req_sum += tl_data_[i].ack_req_estimation;
                tl_data_[i].ack_req_estimation = 0;
            }
            ack_req_sum /= factor;
            if (ack_req_sum == 0) {
                usleep(1000);
                continue;
            }
            if (!is_training) {
                ++exec_epochs;
                if (exec_epochs == kExecutionEpochs) {
                    is_training = true;
                    best_ack_req = 0;
                    best_max_credit = -1;
                    credit_delta = manager_.config().initial_credit - credit_bound;
                    credit_bound = manager_.config().initial_credit;
                }
            } else {
                if (credit_bound == kInfCreditValue) {
                    if (best_ack_req < ack_req_sum * kInfCreditWeight) {
                        // SDS_INFO("set max_credit = inf");
                    } else {
                        credit_delta = best_max_credit - credit_bound;
                        credit_bound = best_max_credit;
                        // SDS_INFO("set max_credit = %d", credit_bound);
                    }
                    is_training = false;
                    exec_epochs = 0;
                } else {
                    ack_req_list[credit_bound] = ack_req_sum;
                    if (best_ack_req < ack_req_sum) {
                        best_ack_req = ack_req_sum;
                        best_max_credit = credit_bound;
                    }
                    if (credit_bound == kMaxCreditValue) {
                        credit_delta = kInfCreditValue - credit_bound;
                    } else {
                        credit_delta = kCreditStep;
                    }
                    credit_bound += credit_delta;
                }
            }
            if (credit_delta) {
                for (auto &tl: tl_data_) {
                    tl.credit.fetch_add(credit_delta, std::memory_order_relaxed);
                }
            }
        }
    }
}
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

#ifndef SDS_INITIATOR_H
#define SDS_INITIATOR_H

#include <infiniband/verbs.h>
#include <functional>
#include <thread>
#include <vector>
#include <atomic>

#include "common.h"
#include "thread.h"
#include "task.h"

#include "global_address.h"
#include "resource_manager.h"
#include "super_chunk.h"

namespace sds {
    class Initiator {
    public:
        Initiator();

        Initiator(void *cache_ptr);

        ~Initiator();

        Initiator(const Initiator &) = delete;

        Initiator &operator=(const Initiator &) = delete;

        enum class Option {
            None, PostRequest, Sync
        };

    public:
        int connect(node_t mem_node_id, const char *domain_name, uint16_t tcp_port, int qp_size) {
            return manager_.connect(mem_node_id, domain_name, tcp_port, qp_size);
        }

        int disconnect(node_t mem_node_id) {
            return manager_.disconnect(mem_node_id);
        }

        GlobalAddress alloc_memory(node_t mem_node_id, size_t size);

        int free_memory(GlobalAddress addr, size_t size);

        void *alloc_cache(size_t size);

        int set_root_entry(node_t mem_node_id, uint8_t index, uint64_t value);

        int get_root_entry(node_t mem_node_id, uint8_t index, uint64_t &value);

        int write(const void *local, const GlobalAddress& remote, size_t length, Option option = Option::None, int flags = 0) {
            int rc = add_request(IBV_WR_RDMA_WRITE, local, remote, length, 0, 0, flags);
            if (rc || option == Option::None) {
                return rc;
            } else if (option == Option::PostRequest) {
                return post_request();
            } else {
                return sync();
            }
        }

        int read(const void *local, const GlobalAddress& remote, size_t length, Option option = Option::None, int flags = 0) {
            RecordReadBytes(length);
            int rc = add_request(IBV_WR_RDMA_READ, local, remote, length, 0, 0, flags);
            if (rc || option == Option::None) {
                return rc;
            } else if (option == Option::PostRequest) {
                return post_request();
            } else {
                return sync();
            }
        }

        int fetch_and_add(const void *local, const GlobalAddress& remote, uint64_t add_val, Option option = Option::None, int flags = 0) {
            int rc = add_request(IBV_WR_ATOMIC_FETCH_AND_ADD, local, remote, sizeof(uint64_t), add_val, 0, flags);
            if (rc || option == Option::None) {
                return rc;
            } else if (option == Option::PostRequest) {
                return post_request();
            } else {
                return sync();
            }
        }

        int compare_and_swap(const void *local, const GlobalAddress& remote, uint64_t compare_val, uint64_t swap_val, Option option = Option::None, int flags = 0) {
            int rc = add_request(IBV_WR_ATOMIC_CMP_AND_SWP, local, remote, sizeof(uint64_t), compare_val, swap_val, flags);
            if (rc || option == Option::None) {
                return rc;
            } else if (option == Option::PostRequest) {
                return post_request();
            } else {
                return sync();
            }
        }

        int post_request();

        int sync();

        int write_sync(const void *local, const GlobalAddress& remote, size_t length) {
            uint32_t rkey;
            uintptr_t remote_va;
            if (manager_.get_remote_memory_key(remote.node, remote.mr_id,remote.offset, length, rkey, remote_va)) {
                return -1;
            }
            ibv_send_wr wr;
            ibv_sge sge;
            sge.length = length;
            sge.lkey = manager_.get_local_memory_lkey(0);
            sge.addr = (uintptr_t) local;
            wr.wr_id = 0;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.num_sge = 1;
            wr.sg_list = &sge;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.next = nullptr;
            wr.wr.rdma.rkey = rkey;
            wr.wr.rdma.remote_addr = remote_va + remote.offset;
            manager_.post_send(remote.node, GetThreadID() % manager_.get_qp_size(remote.node), &wr);
            std::vector<uint64_t> wr_id;
            while (manager_.do_poll(remote.node, GetThreadID() % manager_.get_qp_size(remote.node), wr_id, 1) != 1);
            return 0;
        }

        int read_sync(const void *local, const GlobalAddress& remote, size_t length) {
            uint32_t rkey;
            uintptr_t remote_va;
            if (manager_.get_remote_memory_key(remote.node, remote.mr_id,remote.offset, length, rkey, remote_va)) {
                return -1;
            }
            ibv_send_wr wr;
            ibv_sge sge;
            sge.length = length;
            sge.lkey = manager_.get_local_memory_lkey(0);
            sge.addr = (uintptr_t) local;
            wr.wr_id = 0;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.num_sge = 1;
            wr.sg_list = &sge;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.next = nullptr;
            wr.wr.rdma.rkey = rkey;
            wr.wr.rdma.remote_addr = remote_va + remote.offset;
            manager_.post_send(remote.node, GetThreadID() % manager_.get_qp_size(remote.node), &wr);
            std::vector<uint64_t> wr_id;
            while (manager_.do_poll(remote.node, GetThreadID() % manager_.get_qp_size(remote.node), wr_id, 1) != 1);
            return 0;
        }

    public:
        // When coroutine is enabled, use this as the polling coroutine
        std::function<void()> get_poll_task(int &running_tasks);

        void run_tasks();

        const SmartConfig &config() const { return manager_.config(); }

        void disable_inline_write() { manager_.config().max_inline_data = 0; }

    private:
        int add_request(ibv_wr_opcode opcode, const void *local, const GlobalAddress& remote, size_t length,
                        uint64_t compare_add, uint64_t swap, int flags);

        int poll_once(node_t mem_node_id, bool notify = false);

        void decrease_credit(int count);

        void increase_credit(int count);

        void tuner_func();

        SuperChunk *find_super_chunk(node_t mem_node_id);

        GlobalAddress do_alloc_memory(node_t mem_node_id, int sz_id);

        GlobalAddress do_alloc_chunk(node_t mem_node_id, int count);

    private:
        const static size_t kNumFreeLists = 12;
        static_assert(kChunkShift - kNumFreeLists == 2, "");
        const size_t kPowerOfTwo[kNumFreeLists] = {8, 16, 32, 64, 128, 256, 512,
                                                   1024, 2048, 4096, 8192, 16384 };
        int kSizeToClass[kChunkSize + 1];

        class FreeList {
        public:
            FreeList() : head_(0), tail_(0), capacity_(kInitialCapacity) {
                entries_ = new uint64_t[capacity_];
            }

            ~FreeList() {
                delete []entries_;
            }

            void enqueue(uint64_t offset) {
                int next_head = head_ + 1;
                if (next_head == capacity_) {
                    next_head = 0;
                }
                if (tail_ == next_head) {
                    resize();
                }
                entries_[head_] = offset;
                head_ = next_head;
            }

            bool empty() {
                return head_ == tail_;
            }

            uint64_t dequeue() {
                assert(!empty());
                uint64_t val = entries_[tail_];
                tail_++;
                if (tail_ == capacity_) {
                    tail_ = 0;
                }
                return val;
            }

        private:
            void resize() {
                int new_capacity = capacity_ * 2;
                uint64_t *new_entries = new uint64_t[new_capacity];
                memcpy(new_entries, entries_, capacity_ * sizeof(uint64_t));
                delete []entries_;
                entries_ = new_entries;
                capacity_ = new_capacity;
            }

        private:
            const static size_t kInitialCapacity = 32;

            int head_, tail_, capacity_;
            uint64_t *entries_;
        };

        struct FreeListArray {
            FreeList sz[kNumFreeLists];
        };

        struct RequestBuffer {
            const static int kCapacity = 16;

            struct ibv_send_wr wr_list[kCapacity];
            struct ibv_sge sge_list[kCapacity];
            int size;
            int mem_node_id;

        public:
            RequestBuffer() : size(0), mem_node_id(-1) {}
        };

        struct QueuePairState {
            std::vector<uint64_t> post_req;
            std::vector<uint64_t> ack_req;
            uint64_t inflight_ack;

        public:
            QueuePairState() : inflight_ack(0) {}
        };

        struct ThreadLocal {
            std::atomic<int> credit;
            volatile uint64_t ack_req_estimation;
            QueuePairState qp_state;
            bool task_lock;
            RequestBuffer req_buf[kMaxTasksPerThread];
            std::unordered_map<node_t, FreeListArray> free_list;
            std::unordered_map<node_t, SuperChunk *> super_chunk;
            std::vector<uint64_t> post_req_snapshot[kMaxTasksPerThread];
        };

    private:
        ResourceManager manager_;
        volatile bool tuner_running_;
        std::thread tuner_;
        void *cache_;
        size_t cache_size_;
        std::atomic<size_t> cache_offset_;
        alignas(128) ThreadLocal tl_data_[kMaxThreads];
    };
}

#endif //SDS_INITIATOR_H

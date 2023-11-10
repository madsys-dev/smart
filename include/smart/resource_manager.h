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

#ifndef SDS_RESOURCE_MANAGER_H
#define SDS_RESOURCE_MANAGER_H

#include <map>
#include <queue>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <infiniband/verbs.h>

#include "common.h"

#include "config.h"
#include "global_address.h"

namespace sds {
    const static int MR_FULL_PERMISSION = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    class ResourceManager {
    public:
        struct MemoryRegionMeta {
            uintptr_t addr;
            size_t length;
            uint32_t rkey;
            int valid;
        };

        struct ExchangeMessage {
            int peer_node_id;
            uint16_t lid;
            uint16_t qp_size;
            uint32_t qp_num[kMaxThreads];
            MemoryRegionMeta mr_list[kMemoryRegions];
        };

        struct CompQueue {
            ibv_cq *cq;

            CompQueue(ibv_cq *cq) : cq(cq) {}

            ~CompQueue() {
                if (cq) {
                    ibv_destroy_cq(cq);
                    cq = nullptr;
                }
            }
        };

        struct QueuePair {
            ibv_qp *qp;
            int class_id;
            QueuePair *next;

        public:
            QueuePair(ibv_qp *qp) : qp(qp), next(nullptr), class_id(-1) {}

            ~QueuePair() {
                if (qp) {
                    ibv_destroy_qp(qp);
                    qp = nullptr;
                }
            }
        };

        struct Resource {
            std::vector<QueuePair *> qp_list;
            std::vector<CompQueue *> cq_list;
            std::vector<QueuePair *> free_list; // size == max_class_id
            std::map<uint64_t, int> class_id_map;
            int next_class_id = 0;
        };

        struct RemoteNode {
            QueuePair **qp_list;
            MemoryRegionMeta peer_mr_list[kMemoryRegions];
            int local_node_id;                  // index of the node_list array
            int peer_node_id;                   // my node id in the remote side, useful in RPC
            int qp_size;                        // sizeof qp_list

        public:
            RemoteNode() : qp_list(nullptr), qp_size(0), local_node_id(-1), peer_node_id(-1) {}

            ~RemoteNode() {
                delete[]qp_list;
            }
        };

        class Listener {
        public:
            using CallbackFunc = std::function<int(RemoteNode *)>;

            static Listener *Start(ResourceManager *manager, uint16_t tcp_port, const CallbackFunc &func);

            ~Listener();

            Listener(const Listener &) = delete;

            Listener &operator=(const Listener &) = delete;

            bool running() const { return running_; }

            void stop();

        private:
            Listener(ResourceManager *context, int listen_fd, const CallbackFunc &func);

            static void thread_wrapper(Listener *listener) {
                if (listener->thread_func()) {
                    exit(EXIT_FAILURE);
                }
            }

            int thread_func();

        private:
            ResourceManager *context_;
            std::thread listener_;
            volatile bool running_;
            int listen_fd_;
            std::queue<int> node_id_queue_;
            CallbackFunc func_;
        };

        ResourceManager();

        ~ResourceManager();

        ResourceManager(const ResourceManager &) = delete;

        ResourceManager &operator=(const ResourceManager &) = delete;

        int register_main_memory(void *addr, size_t length, int perm);

        int register_device_memory(size_t length, int perm);

        int copy_from_device_memory(void *dst_addr, uint64_t src_offset, size_t length);

        int copy_to_device_memory(uint64_t dst_offset, void *src_addr, size_t length);

        int connect(node_t node_id, const char *domain_name, uint16_t tcp_port, int qp_size);

        int disconnect(node_t node_id);

        uint32_t get_local_memory_lkey(mr_id_t mr_id);

        uint32_t get_local_memory_rkey(mr_id_t mr_id);

        int get_remote_memory_key(node_t node_id, mr_id_t mr_id, uint64_t offset, size_t length, uint32_t &rkey, uintptr_t &addr);

        int post_send(node_t node_id, int qp_idx, ibv_send_wr *wr_list);

        int do_poll(node_t node_id, int qp_idx, std::vector<uint64_t> &wr_id_list, int count = 16);

        int get_peer_node_id(node_t node_id) const;

        int get_qp_size(node_t node_id) const;

        int max_node_id() const { return max_node_id_.load(std::memory_order_acquire); }

        const SmartConfig &config() const { return config_; }

        SmartConfig &config() { return config_; }

    private:
        int open_mlx_device(const char *device_name);

        static int tcp_connect(const char *domain_name, uint16_t tcp_port);

        static int tcp_synchronize(int fd, bool active_side);

        int send_local_info(RemoteNode &node, int fd, int qp_size = -1);

        int receive_peer_info(RemoteNode &node, int fd);

        QueuePair *allocate_queue_pair(int affinity);

        int create_queue_pair();

        int get_class_id(QueuePair *qp);

        int enable_queue_pair(QueuePair *qp, uint16_t lid, uint32_t qp_num, uint32_t psn);

        int free_queue_pair(QueuePair *qp);

        int do_connect(int node_id, int fd, int qp_size);

        int do_accept(int node_id, int fd);

    private:
        struct ibv_context *ib_ctx_;
        struct ibv_pd *ib_pd_;
        struct ibv_mr *mr_list_[kMemoryRegions];
        struct ibv_dm *dm_list_[kMemoryRegions];
        RemoteNode *node_list_;
        Resource resource_;
        uint16_t ib_lid_;
        uint8_t ib_port_;
        std::atomic<int> max_node_id_;
        SmartConfig config_;
    };
}

#endif //SDS_RESOURCE_MANAGER_H

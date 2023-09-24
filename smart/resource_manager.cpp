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

#include <infiniband/mlx5dv.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/poll.h>

#include "smart/resource_manager.h"

namespace sds {
    ResourceManager::ResourceManager() : max_node_id_(-1) {
        ibv_port_attr port_attr_;
        ibv_device_attr device_attr_;
        std::fill(mr_list_, mr_list_ + kMemoryRegions, nullptr);
        std::fill(dm_list_, dm_list_ + kMemoryRegions, nullptr);
        node_list_ = new RemoteNode[config_.max_nodes];
        resource_.free_list.resize(config_.total_uuars, nullptr);
        ib_port_ = config_.infiniband_port;
        if (open_mlx_device(config_.infiniband_name.c_str())) {
            SDS_WARN("open mlx5 device failed");
            exit(EXIT_FAILURE);
        }
        if (ibv_query_port(ib_ctx_, ib_port_, &port_attr_)) {
            SDS_PERROR("ibv_query_port");
            exit(EXIT_FAILURE);
        }
        if (ibv_query_device(ib_ctx_, &device_attr_)) {
            SDS_PERROR("ibv_query_device");
            exit(EXIT_FAILURE);
        }
        assert(device_attr_.atomic_cap != IBV_ATOMIC_NONE);
        // SDS_INFO("atomic_cap %d max qp %d", device_attr_.atomic_cap, device_attr_.max_qp);
        ib_lid_ = port_attr_.lid;
        ib_pd_ = ibv_alloc_pd(ib_ctx_);
        if (!ib_pd_) {
            SDS_PERROR("ibv_alloc_pd");
            exit(EXIT_FAILURE);
        }
    }

    ResourceManager::~ResourceManager() {
        resource_.free_list.clear();
        for (auto entry: resource_.qp_list) {
            delete entry;
        }
        resource_.qp_list.clear();
        for (auto entry: resource_.cq_list) {
            delete entry;
        }
        resource_.cq_list.clear();
        delete[]node_list_;
        for (int i = 0; i < kMemoryRegions; ++i) {
            if (mr_list_[i]) {
                ibv_dereg_mr(mr_list_[i]);
            }
            if (dm_list_[i]) {
                ibv_free_dm(dm_list_[i]);
            }
        }
        if (ib_pd_) {
            ibv_dealloc_pd(ib_pd_);
            ib_pd_ = nullptr;
        }
        if (ib_ctx_) {
            ibv_close_device(ib_ctx_);
            ib_ctx_ = nullptr;
        }
    }

    int ResourceManager::register_main_memory(void *addr, size_t length, int perm) {
        ibv_mr *mr = ibv_reg_mr(ib_pd_, addr, length, perm);
        if (!mr) {
            SDS_PERROR("ibv_reg_mr");
            return -1;
        }
        assert(!mr_list_[MAIN_MEMORY_MR_ID]);
        mr_list_[MAIN_MEMORY_MR_ID] = mr;
        return 0;
    }

    int ResourceManager::register_device_memory(size_t length, int perm) {
        struct ibv_alloc_dm_attr attr;
        attr.length = length;
        attr.log_align_req = 3; // 8-byte aligned
        attr.comp_mask = 0;
        ibv_dm *dm = ibv_alloc_dm(ib_ctx_, &attr);
        if (!dm) {
            SDS_PERROR("ibv_alloc_dm");
            return -1;
        }

        char *buf = new char[length];
        memset(buf, 0, length);
        if (ibv_memcpy_to_dm(dm, 0, buf, length)) {
            SDS_PERROR("ibv_memcpy_to_dm");
            ibv_free_dm(dm);
            return -1;
        }
        delete[]buf;

        perm |= IBV_ACCESS_ZERO_BASED;
        ibv_mr *mr = ibv_reg_dm_mr(ib_pd_, dm, 0, length, perm);
        if (!mr) {
            SDS_PERROR("ibv_reg_dm_mr");
            ibv_free_dm(dm);
            return -1;
        }
        assert(!mr_list_[DEVICE_MEMORY_MR_ID]);
        dm_list_[DEVICE_MEMORY_MR_ID] = dm;
        mr_list_[DEVICE_MEMORY_MR_ID] = mr;
        return 0;
    }

    int ResourceManager::copy_from_device_memory(void *dst_addr, uint64_t src_offset, size_t length) {
        assert(dm_list_[DEVICE_MEMORY_MR_ID]);
        int rc = ibv_memcpy_from_dm(dst_addr, dm_list_[DEVICE_MEMORY_MR_ID], src_offset, length);
        if (rc) {
            SDS_PERROR("ibv_memcpy_from_dm");
        }
        return rc;
    }

    int ResourceManager::copy_to_device_memory(uint64_t dst_offset, void *src_addr, size_t length) {
        assert(dm_list_[DEVICE_MEMORY_MR_ID]);
        int rc = ibv_memcpy_to_dm(dm_list_[DEVICE_MEMORY_MR_ID], dst_offset, src_addr, length);
        if (rc) {
            SDS_PERROR("ibv_memcpy_to_dm");
        }
        return rc;
    }

    int ResourceManager::connect(node_t node_id, const char *domain_name, uint16_t tcp_port, int qp_size) {
        assert(node_id >= 0 && node_id < config_.max_nodes);
        if (node_id > max_node_id()) {
            max_node_id_.store(node_id, std::memory_order_release);
        }
        auto &node = node_list_[node_id];
        if (node.qp_list) {
            SDS_WARN("memory node %d has been used", node_id);
            return -1;
        }

        int fd = tcp_connect(domain_name, tcp_port);
        if (fd < 0) {
            return -1;
        }

        int rc = do_connect(node_id, fd, qp_size);
        close(fd);
        return rc;
    }

    int ResourceManager::disconnect(node_t node_id) {
        assert(node_id >= 0 && node_id < config_.max_nodes);
        auto &node = node_list_[node_id];
        assert(node.qp_list);
        for (int i = 0; i < node.qp_size; ++i) {
            free_queue_pair(node.qp_list[i]);
        }
        delete[]node.qp_list;
        new(&node) RemoteNode();
        return 0;
    }

    uint32_t ResourceManager::get_local_memory_lkey(mr_id_t mr_id) {
        assert(mr_id < kMemoryRegions && mr_list_[mr_id]);
        return mr_list_[mr_id]->lkey;
    }

    uint32_t ResourceManager::get_local_memory_rkey(mr_id_t mr_id) {
        assert(mr_id < kMemoryRegions && mr_list_[mr_id]);
        return mr_list_[mr_id]->rkey;
    }

    int ResourceManager::get_remote_memory_key(node_t node_id, mr_id_t mr_id, uint64_t offset, size_t length,
                                               uint32_t &rkey, uintptr_t &addr) {
        assert(node_id < config_.max_nodes && mr_id < kMemoryRegions);
        auto &entry = node_list_[node_id].peer_mr_list[mr_id];
        assert(entry.valid && offset + length <= entry.length);
        rkey = entry.rkey;
        addr = entry.addr;
        return 0;
    }

    int ResourceManager::post_send(node_t node_id, int qp_idx, ibv_send_wr *wr_list) {
        ibv_send_wr *bad_wr;
        assert(node_id < config_.max_nodes);
        auto &node = node_list_[node_id];
        assert(qp_idx >= 0 && qp_idx < node.qp_size);
        if (ibv_post_send(node.qp_list[qp_idx]->qp, wr_list, &bad_wr)) {
            SDS_PERROR("ibv_post_send");
            return -1;
        }
        return 0;
    }

    int ResourceManager::do_poll(node_t node_id, int qp_idx, std::vector<uint64_t> &wr_id_list, int count) {
        struct ibv_wc wc_list[count];
        assert(node_id >= 0 && node_id < config_.max_nodes);
        auto &node = node_list_[node_id];
        assert(qp_idx >= 0 && qp_idx < node.qp_size);
        int rc = ibv_poll_cq(node.qp_list[qp_idx]->qp->send_cq, count, wc_list);
        if (rc < 0) {
            SDS_PERROR("ibv_poll_cq");
            return -1;
        }
        if (rc == 0) {
            return 0;
        }
        bool has_failed = false;
        wr_id_list.clear();
        for (int i = 0; i < rc; ++i) {
            if (wc_list[i].status != IBV_WC_SUCCESS) {
                SDS_WARN("detected wc status error: %s, vendor error %x",
                         ibv_wc_status_str(wc_list[i].status), wc_list[i].vendor_err);
                has_failed = true;
            }
            wr_id_list.push_back(wc_list[i].wr_id);
        }
        if (has_failed) {
            return -1;
        }
        return rc;
    }

    int ResourceManager::get_peer_node_id(node_t node_id) const {
        assert(node_id >= 0 && node_id < config_.max_nodes);
        return node_list_[node_id].peer_node_id;
    }

    int ResourceManager::get_qp_size(node_t node_id) const {
        assert(node_id >= 0 && node_id < config_.max_nodes);
        return node_list_[node_id].qp_size;
    }

    int ResourceManager::open_mlx_device(const char *device_name) {
        int num_devices = 0;
        ibv_device **device_list = ibv_get_device_list(&num_devices);
        if (!device_list || num_devices <= 0) {
            return -1;
        }
        if (strlen(device_name) == 0) {
            assert(mlx5dv_is_supported(device_list[0]));
            ib_ctx_ = ibv_open_device(device_list[0]);
            ibv_free_device_list(device_list);
            return ib_ctx_ ? 0 : -1;
        } else {
            for (int i = 0; i < num_devices; ++i) {
                const char *target_device_name = ibv_get_device_name(device_list[i]);
                if (target_device_name && strcmp(target_device_name, device_name) == 0) {
                    assert(mlx5dv_is_supported(device_list[i]));
                    ib_ctx_ = ibv_open_device(device_list[i]);
                    if (ib_ctx_) {
                        ibv_free_device_list(device_list);
                        return 0;
                    }
                }
            }
        }
        ibv_free_device_list(device_list);
        return -1;
    }

    int ResourceManager::tcp_connect(const char *domain_name, uint16_t tcp_port) {
        timeval timeout;
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        int conn_fd = -1;
        int on = 1;
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        char service[16];
        sprintf(service, "%u", tcp_port);

        if (getaddrinfo(domain_name, service, &hints, &result)) {
            SDS_PERROR("getaddrinfo");
            return -1;
        }

        for (rp = result; rp; rp = rp->ai_next) {
            conn_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (conn_fd == -1) {
                continue;
            }

            if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) ||
                setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout))) {
                SDS_PERROR("setsockopt");
                close(conn_fd);
                conn_fd = -1;
                continue;
            }

            if (::connect(conn_fd, rp->ai_addr, rp->ai_addrlen)) {
                SDS_PERROR("connect");
                close(conn_fd);
                conn_fd = -1;
                continue;
            }
        }

        freeaddrinfo(result);
        return conn_fd;
    }

    int ResourceManager::tcp_synchronize(int fd, bool active_side) {
        char send_buf = active_side ? 'A' : 'B';
        char recv_buf;
        if (fd < 0) {
            return -EINVAL;
        }
        if (active_side) {
            if (read(fd, &recv_buf, 1) != 1 || recv_buf != 'B') return -1;
            if (write(fd, &send_buf, 1) != 1) return -1;
        } else {
            if (write(fd, &send_buf, 1) != 1) return -1;
            if (read(fd, &recv_buf, 1) != 1 || recv_buf != 'A') return -1;
        }
        return 0;
    }

    static ssize_t write_fully(int fd, const void *buf, size_t len) {
        char *pos = (char *) buf;
        size_t nbytes = len;
        while (nbytes) {
            ssize_t rc = write(fd, pos, nbytes);
            if (rc < 0) {
                return rc;
            }
            pos += rc;
            nbytes -= rc;
        }
        return len;
    }

    static ssize_t read_fully(int fd, void *buf, size_t len) {
        char *pos = (char *) buf;
        size_t nbytes = len;
        while (nbytes) {
            ssize_t rc = read(fd, pos, nbytes);
            if (rc < 0) {
                return rc;
            }
            pos += rc;
            nbytes -= rc;
        }
        return len;
    }

    int ResourceManager::send_local_info(RemoteNode &node, int fd, int qp_size) {
        ExchangeMessage msg;
        const size_t msg_size = sizeof(ExchangeMessage);

        if (!node.qp_size) {
            assert(qp_size > 0);
            node.qp_size = qp_size;
            node.qp_list = new QueuePair *[node.qp_size];
            for (int i = 0; i < node.qp_size; ++i) {
                node.qp_list[i] = nullptr;
            }
            for (int i = 0; i < node.qp_size; ++i) {
                node.qp_list[i] = allocate_queue_pair(i);
                if (!node.qp_list[i]) {
                    return -1;
                }
            }
        }

        memset(&msg, 0, msg_size);
        msg.peer_node_id = (int) (&node - node_list_);
        msg.lid = ib_lid_;
        msg.qp_size = node.qp_size;

        for (int i = 0; i < node.qp_size; ++i) {
            msg.qp_num[i] = node.qp_list[i]->qp->qp_num;
        }
        for (int i = 0; i < kMemoryRegions; ++i) {
            if (mr_list_[i]) {
                msg.mr_list[i].length = mr_list_[i]->length;
                msg.mr_list[i].rkey = mr_list_[i]->rkey;
                msg.mr_list[i].addr = (uintptr_t) mr_list_[i]->addr;
                msg.mr_list[i].valid = 1;
            }
        }
        if (write_fully(fd, &msg, msg_size) != msg_size) {
            SDS_PERROR("write");
            return -1;
        }
        return 0;
    }

    int ResourceManager::receive_peer_info(RemoteNode &node, int fd) {
        ExchangeMessage msg;
        const size_t msg_size = sizeof(ExchangeMessage);
        if (read_fully(fd, &msg, msg_size) != msg_size) {
            SDS_PERROR("read");
            return -1;
        }

        if (!node.qp_size) {
            node.qp_size = msg.qp_size;
            node.qp_list = new QueuePair *[node.qp_size];
            for (int i = 0; i < node.qp_size; ++i) {
                node.qp_list[i] = nullptr;
            }
            for (int i = 0; i < node.qp_size; ++i) {
                node.qp_list[i] = allocate_queue_pair(i);
                if (!node.qp_list[i]) {
                    return -1;
                }
            }
        }

        for (int i = 0; i < node.qp_size; ++i) {
            if (enable_queue_pair(node.qp_list[i], msg.lid, msg.qp_num[i], i << 16)) {
                return -1;
            }
        }

        for (int i = 0; i < kMemoryRegions; ++i) {
            node.peer_mr_list[i] = msg.mr_list[i];
        }

        node.peer_node_id = msg.peer_node_id;
        node.local_node_id = (int) (&node - node_list_);
        return 0;
    }

    ResourceManager::QueuePair *ResourceManager::allocate_queue_pair(int affinity) {
        QueuePair *qp;
        int loop_count = 0;

        if (!config_.uuar_affinity_enabled) {
            affinity = -1;
        }

        start:
        loop_count++;
        assert(loop_count < config_.total_uuars);

        int start_class = 0, step = 1;
        if (affinity >= 0) {
            start_class = config_.private_uuars + affinity % config_.shared_uuars;
            step += config_.total_uuars; // static mapping
        }

        for (int i = start_class; i < config_.total_uuars; i += step) {
            qp = resource_.free_list[i];
            if (qp) {
                resource_.free_list[i] = qp->next;
                qp->next = nullptr;
                goto end;
            }
        }

        if (create_queue_pair()) {
            return nullptr;
        }

        goto start;

        end:
        return qp;
    }

    int ResourceManager::create_queue_pair() {
        ibv_qp_init_attr attr;
        ibv_qp *qp;
        ibv_cq *cq;
        memset(&attr, 0, sizeof(attr));
        int class_id = resource_.next_class_id;
        resource_.next_class_id++;
        if (resource_.next_class_id == config_.total_uuars) {
            resource_.next_class_id = config_.private_uuars;
        }
        if (config_.shared_cq) {
            if (resource_.cq_list.size() < config_.total_uuars) {
                cq = ibv_create_cq(ib_ctx_, config_.max_cqe_size, nullptr, nullptr, 0);
                if (!cq) {
                    SDS_PERROR("ibv_create_cq");
                    return -1;
                }
                resource_.cq_list.push_back(new CompQueue(cq));
            } else {
                cq = resource_.cq_list[class_id]->cq;
            }
        } else {
            cq = ibv_create_cq(ib_ctx_, config_.max_cqe_size, nullptr, nullptr, 0);
            if (!cq) {
                SDS_PERROR("ibv_create_cq");
                return -1;
            }
            resource_.cq_list.push_back(new CompQueue(cq));
        }
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.sq_sig_all = false;
        attr.qp_type = IBV_QPT_RC;
        attr.cap.max_send_wr = attr.cap.max_recv_wr = config_.max_wqe_size;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = config_.max_sge_size;
        attr.cap.max_inline_data = config_.max_inline_data;
        qp = ibv_create_qp(ib_pd_, &attr);
        if (!qp) {
            SDS_PERROR("ibv_create_qp");
            return -1;
        }
        auto entry = new QueuePair(qp);
        // if (class_id != get_class_id(entry)) {
        //     SDS_INFO("FUCK %d %d", class_id, get_class_id(entry));
        // }
        // assert(class_id == get_class_id(entry));
        // class_id = get_class_id(entry);
        // if (class_id < 0) {
        //     return -1;
        // }
        entry->next = resource_.free_list[class_id];
        resource_.free_list[class_id] = entry;
        resource_.qp_list.push_back(entry);
        return 0;
    }

    int ResourceManager::get_class_id(QueuePair *qp) {
        mlx5dv_obj obj;
        mlx5dv_qp out;
        memset(&obj, 0, sizeof(mlx5dv_obj));
        obj.qp.in = qp->qp;
        obj.qp.out = &out;
        if (mlx5dv_init_obj(&obj, MLX5DV_OBJ_QP)) {
            SDS_PERROR("mlx5dv_init_obj");
            return -1;
        }
        uint64_t reg_addr = (uint64_t) out.bf.reg;
        if (resource_.class_id_map.count(reg_addr)) {
            return resource_.class_id_map[reg_addr];
        } else {
            int next_id = resource_.class_id_map.size();
            resource_.class_id_map[reg_addr] = next_id;
            return next_id;
        }
    }

    int ResourceManager::enable_queue_pair(QueuePair *qp, uint16_t lid, uint32_t qp_num, uint32_t psn) {
        ibv_qp_attr attr;
        int flags;

        // Any -> RESET
        flags = IBV_QP_STATE;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RESET;
        if (ibv_modify_qp(qp->qp, &attr, flags)) {
            SDS_PERROR("ibv_modify_qp");
            return -1;
        }

        // RESET -> INIT
        flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = ib_port_;
        attr.pkey_index = 0;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

        if (ibv_modify_qp(qp->qp, &attr, flags)) {
            SDS_PERROR("ibv_modify_qp");
            return -1;
        }

        // INIT -> RTR
        flags = IBV_QP_STATE | IBV_QP_PATH_MTU;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = IBV_MTU_512;
        if (qp->qp->qp_type == IBV_QPT_RC) {
            flags |= IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                     IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
            attr.ah_attr.is_global = 0;
            attr.ah_attr.dlid = lid;
            attr.ah_attr.port_num = ib_port_;
            attr.dest_qp_num = qp_num;
            attr.rq_psn = psn; // match remote sq_psn
            attr.max_dest_rd_atomic = 16;
            attr.min_rnr_timer = 12;
        }

        if (ibv_modify_qp(qp->qp, &attr, flags)) {
            SDS_PERROR("ibv_modify_qp");
            return -1;
        }

        // RTR -> RTS
        flags = IBV_QP_STATE;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        if (qp->qp->qp_type == IBV_QPT_RC) {
            flags |= IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                     IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
            attr.timeout = 14;
            attr.retry_cnt = 7;
            attr.rnr_retry = 7;
            attr.sq_psn = psn;
            attr.max_rd_atomic = 16;
        }

        if (ibv_modify_qp(qp->qp, &attr, flags)) {
            SDS_PERROR("ibv_modify_qp");
            return -1;
        }

        return 0;
    }

    int ResourceManager::free_queue_pair(QueuePair *qp) {
        if (!qp) return 0;
        ibv_qp_attr attr;
        int flags = IBV_QP_STATE;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RESET;
        if (ibv_modify_qp(qp->qp, &attr, flags)) {
            SDS_PERROR("ibv_modify_qp");
        }
        int class_id = get_class_id(qp);
        if (class_id < 0) {
            return -1;
        }
        qp->next = resource_.free_list[class_id];
        resource_.free_list[class_id] = qp;
        return 0;
    }

    int ResourceManager::do_connect(int node_id, int fd, int qp_size) {
        auto &node = node_list_[node_id];
        if (send_local_info(node, fd, qp_size)) {
            goto end;
        }

        if (receive_peer_info(node, fd)) {
            goto end;
        }

        if (tcp_synchronize(fd, false)) {
            goto end;
        }

        return 0;

        end:
        for (int i = 0; i < node.qp_size; ++i) {
            if (node.qp_list[i]) {
                free_queue_pair(node.qp_list[i]);
            }
        }
        delete[]node.qp_list;
        return -1;
    }

    int ResourceManager::do_accept(int node_id, int fd) {
        if (node_id > max_node_id()) {
            max_node_id_.store(node_id, std::memory_order_release);
        }
        auto &node = node_list_[node_id];
        if (node.qp_list) {
            disconnect(node_id);
        }

        if (receive_peer_info(node, fd)) {
            goto end;
        }

        if (send_local_info(node, fd)) {
            goto end;
        }

        if (tcp_synchronize(fd, true)) {
            goto end;
        }

        return 0;
    end:
        for (int i = 0; i < node.qp_size; ++i) {
            if (node.qp_list[i]) {
                free_queue_pair(node.qp_list[i]);
            }
        }
        delete[]node.qp_list;
        return -1;
    }

    ResourceManager::Listener *
    ResourceManager::Listener::Start(ResourceManager *manager, uint16_t tcp_port, const CallbackFunc &func) {
        sockaddr_in bind_address;
        int on = 1;
        int listen_fd;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(tcp_port);
        bind_address.sin_addr.s_addr = INADDR_ANY;

        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            SDS_PERROR("socket");
            return nullptr;
        }

        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            SDS_PERROR("setsockopt");
            close(listen_fd);
            return nullptr;
        }

        if (bind(listen_fd, (sockaddr *) &bind_address, sizeof(sockaddr_in)) < 0) {
            SDS_PERROR("bind");
            close(listen_fd);
            return nullptr;
        }

        if (listen(listen_fd, 5)) {
            SDS_PERROR("listen");
            close(listen_fd);
            return nullptr;
        }

        return new Listener(manager, listen_fd, func);
    }

    ResourceManager::Listener::Listener(ResourceManager *context, int listen_fd, const CallbackFunc &func) :
            context_(context), listen_fd_(listen_fd), func_(func) {
        for (int i = 0; i < context_->config_.max_nodes; ++i) {
            node_id_queue_.push(i);
        }
        running_ = true;
        listener_ = std::thread(thread_wrapper, this);
    }

    ResourceManager::Listener::~Listener() {
        if (running_) {
            running_ = false;
            listener_.join();
            close(listen_fd_);
        }
    }

    int ResourceManager::Listener::thread_func() {
        sockaddr_in sin;
        socklen_t sin_size = sizeof(sockaddr_in);
        int conn_fd;
        timeval timeout;
        timeout.tv_sec = 120;
        timeout.tv_usec = 0;

        struct pollfd fd;
        fd.fd = listen_fd_;
        fd.events = POLLIN;

        while (running_) {
            int rc = poll(&fd, 1, 1000);
            if (rc < 0) {
                SDS_PERROR("poll");
                return -1;
            }
            if (rc == 0) {
                continue;
            }

            conn_fd = accept(listen_fd_, (sockaddr *) &sin, &sin_size);
            if (conn_fd < 0) {
                SDS_PERROR("accept");
                return -1;
            }

            if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout))) {
                SDS_PERROR("setsockopt");
                close(conn_fd);
                return -1;
            }

            int node_id = node_id_queue_.front();
            node_id_queue_.pop();
            node_id_queue_.push(node_id);

            if (context_->do_accept(node_id, conn_fd)) {
                close(conn_fd);
                return -1;
            }

            close(conn_fd);
            if (func_ && func_(&context_->node_list_[node_id])) {
                return -1;
            }

            SDS_INFO("new connection arrived: local node id %d, peer node id %d",
                     node_id, context_->node_list_[node_id].peer_node_id);
        }

        return 0;
    }

    void ResourceManager::Listener::stop() {
        if (running_) {
            running_ = false;
            listener_.join();
        }
    }
}

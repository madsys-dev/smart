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

#include <iostream>
#include <cassert>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <immintrin.h>
#include <atomic>
#include <random>

#include "smart/thread.h"

#include "smart/initiator.h"
#include "smart/target.h"

using namespace sds;

static const size_t MEM_POOL_SIZE = (1ull << 30);

void run_server(uint16_t port) {
    Target target;
    char *local_addr = (char *) mmap_huge_page(MEM_POOL_SIZE);
    memset(local_addr, 0, MEM_POOL_SIZE);
    int rc = target.register_main_memory(local_addr, MEM_POOL_SIZE);
    assert(!rc);
    rc = target.start(port);
    assert(!rc);
    SDS_INFO("server starts, press c to exit");
    while (getchar() != 'c');
    target.stop();
}

size_t connections = 1;
size_t nr_nodes = 1;
int nr_threads = 1;
size_t block_size = 64;
size_t depth = 1;
int ib_socket = 1;
uint64_t interval = 1000;
int sockets;
int processors_per_socket;
int qp_num;
std::string dump_file_path;
std::string dump_prefix;
std::string type;

std::atomic<uint64_t> total_attempts(0);
volatile int stop_signal = 0;
pthread_barrier_t barrier;
Initiator *node[32];

void *test_thread_func(void *arg) {
    int thread_id = (int) (uintptr_t) arg;
    auto ctx = node[thread_id % nr_nodes];
    BindCore(thread_id);
    size_t kSegmentSize = MEM_POOL_SIZE / nr_threads;
    kSegmentSize &= ~4095ull;
    size_t align_size = block_size < 64 ? 64 : block_size;
    char *buf = (char *) ctx->alloc_cache(align_size * 64);
    uint64_t attempts = 0;
    pthread_barrier_wait(&barrier);
    uint64_t tokens = depth;
    std::mt19937 rnd;
    std::mt19937 rnd2(0);
    std::uniform_int_distribution<uint64_t> dist(0, kSegmentSize / block_size - 1);
    std::uniform_int_distribution<uint64_t> dist2(36, 95);
    uint64_t ts = rdtsc();
    bool enable = true;
    while (!stop_signal) {
        if (rdtsc() - ts > interval * 2400000ull) {
            int tmp = dist2(rnd2);
            enable = (thread_id < tmp);
            ts = rdtsc();
            // if (thread_id == 10 || thread_id == 15) {
            //     printf("%d\n", tmp);
            // }
        }
        uint64_t offset = thread_id * kSegmentSize + block_size * (dist(rnd));
        GlobalAddress remote_addr(attempts % connections, offset);
        int rc = 0;
        if (enable) {
            rc = ctx->read(buf + align_size * tokens, remote_addr, block_size, Initiator::Option::PostRequest);
        } else {
            _mm_pause();
            continue;
        }
        attempts++;
        assert(!rc);
        --tokens;
        while (tokens == 0) {
            rc = ctx->sync();
            assert(!rc);
            tokens = depth;
        }
    }

    pthread_barrier_wait(&barrier);
    total_attempts.fetch_add(attempts);
    return NULL;
}

double connect_time = 0.0;

void report(uint64_t elapsed_time) {
    auto bandwidth = total_attempts * block_size / elapsed_time / 1024.0 / 1024.0;
    auto throughput = total_attempts / elapsed_time / 1000000.0;
    SDS_INFO("%s: #threads=%d, #depth=%ld, #block_size=%ld, BW=%.3lf MB/s, IOPS=%.3lf M/s, conn establish time=%.3lf ms",
             dump_prefix.c_str(), nr_threads, depth, block_size, bandwidth, throughput, connect_time);
    if (dump_file_path.empty()) {
        return;
    }
    FILE *fout = fopen(dump_file_path.c_str(), "a+");
    if (!fout) {
        SDS_PERROR("fopen");
        return;
    }
    fprintf(fout, "%s, %d, %ld, %ld, %.3lf, %.3lf, %.3lf\n",
            dump_prefix.c_str(), nr_threads, depth, block_size, bandwidth, throughput, connect_time);
    fclose(fout);
}

void run_client(const std::vector<std::string> &server_list, uint16_t port) {
    struct timeval start_tv, end_tv;
    pthread_t tid[kMaxThreads];
    double elapsed_time;
    int qp_count = nr_threads;
    if (qp_num > 0) {
        qp_count = qp_num;
    }
    if (qp_num < 0) {
        qp_count = (nr_threads - qp_num - 1) / -qp_num;
    }
    gettimeofday(&start_tv, NULL);
    for (int i = 0; i < nr_nodes; ++i) {
        node[i] = new Initiator();
        node[i]->disable_inline_write();
        for (int j = 0; j < connections; ++j) {
            int rc = node[i]->connect(j, server_list[j % server_list.size()].c_str(),
                                      port, qp_count);
            assert(!rc);
        }
    }
    gettimeofday(&end_tv, NULL);
    connect_time = (end_tv.tv_sec - start_tv.tv_sec) * 1000.0 +
                   (end_tv.tv_usec - start_tv.tv_usec) / 1000.0;
    SDS_INFO("connection established, %.3lf msec", connect_time);
    pthread_barrier_init(&barrier, NULL, nr_threads + 1);
    for (long i = 0; i < nr_threads; ++i) {
        pthread_create(&tid[i], NULL, test_thread_func, (void *) i);
    }
    pthread_barrier_wait(&barrier);
    gettimeofday(&start_tv, NULL);
    sleep(15);
    stop_signal = 1;
    pthread_barrier_wait(&barrier);
    gettimeofday(&end_tv, NULL);
    for (int i = 0; i < nr_threads; ++i) {
        pthread_join(tid[i], NULL);
    }
    pthread_barrier_destroy(&barrier);
    elapsed_time = (end_tv.tv_sec - start_tv.tv_sec) * 1.0 +
                   (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    report(elapsed_time);

    for (int i = 0; i < nr_nodes; ++i) {
        for (int j = 0; j < connections; ++j) {
            node[i]->disconnect(j);
        }
        delete node[i];
    }
}

int main(int argc, char **argv) {
    const char *env_path = getenv("TEST_RDMA_CONF");
    JsonConfig config = JsonConfig::load_file(env_path ? env_path : ROOT_DIR "/config/test_rdma.json");
    ib_socket = (int) config.get("ib_socket").get_int64();
    sockets = (int) config.get("sockets").get_int64();
    processors_per_socket = (int) config.get("processors_per_socket").get_int64();
    qp_num = (int) config.get("qp_num").get_int64();
    int port = (int) config.get("port").get_int64();
    dump_file_path = config.get("dump_file_path").get_str();
    if (getenv("DUMP_FILE_PATH")) {
        dump_file_path = getenv("DUMP_FILE_PATH");
    }
    type = config.get("type").get_str();
    if (getenv("TYPE")) {
        type = getenv("TYPE");
    }
    if (getenv("DUMP_PREFIX")) {
        dump_prefix = std::string(getenv("DUMP_PREFIX"));
    } else {
        dump_prefix = "rdma-" + type;
    }
    BindCore(0);
    if (argc == 1) {
        run_server(port);
    } else {
        block_size = (int) config.get("block_size").get_int64();
        if (getenv("BLKSIZE")) {
            block_size = (int) atoi(getenv("BLKSIZE"));
        }
        nr_threads = argc < 2 ? 1 : atoi(argv[1]);
        depth = argc < 3 ? 1 : atoi(argv[2]);
        interval = argc < 4 ? 1000 : atoi(argv[3]);
        // connections = argc < 4 ? 1 : atoi(argv[3]);
        // ib_socket = argc < 5 ? ib_socket : atoi(argv[4]);
        std::vector<std::string> server_list;
        JsonConfig servers = config.get("servers");
        for (int i = 0; i < servers.size(); ++i) {
            server_list.push_back(servers.get(i).get_str());
        }
        assert(!server_list.empty());
        run_client(server_list, port);
    }
    return 0;
}

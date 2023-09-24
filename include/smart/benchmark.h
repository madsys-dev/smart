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

#ifndef SDS_BENCHMARK_H
#define SDS_BENCHMARK_H

#include "util/ycsb.h"
#include "smart/task.h"
#include "util/json_config.h"
#include "smart/initiator.h"

#include <thread>
#include <sstream>
#include <unistd.h>
#include <sys/stat.h>
#include <functional>
#include <iomanip>
#include <algorithm>

// #define DISK_DATASET

namespace sds {
    namespace datastructure {
        template<class T>
        class BenchmarkRunner {
        public:
            const static uint64_t kCpuFrequency = 2400;
            uint64_t g_idle_cycles = 0;

            void IdleExecution() {
                if (g_idle_cycles) {
                    uint64_t start_tsc = rdtsc();
                    while (rdtsc() - start_tsc < g_idle_cycles) {
                        YieldTask();
                    }
                }
            }

        public:
            using OpRecord = util::OpRecord;

            BenchmarkRunner(JsonConfig &config, int argc, char **argv)
                    : config_(config), index_(nullptr), records_(nullptr), global_ops_(0) {
                nr_threads_ = (int) config.get("nr_threads").get_int64();
                tasks_per_thread_ = (int) config.get("tasks_per_thread").get_int64();
                dataset_ = config_.get("dataset").get_str();
                if (getenv("DATASET_PATH")) {
                    dataset_ = getenv("DATASET_PATH");
                }
                dump_file_path_ = config.get("dump_file_path").get_str();
                if (getenv("DUMP_FILE_PATH")) {
                    dump_file_path_ = getenv("DUMP_FILE_PATH");
                }

                nic_numa_node_ = (int) config.get("nic_numa_node").get_int64();
                cpu_nodes_ = (int) config.get("cpu_nodes").get_int64();
                cores_per_cpu_ = (int) config.get("cores_per_cpu").get_int64();

                insert_before_execution_ = config.get("insert_before_execution").get_bool();
                max_key_ = config.get("max_key").get_int64();
                key_length_ = config.get("key_length").get_int64();
                value_length_ = config.get("value_length").get_int64();
                rehash_key_ = config.get("rehash_key").get_bool();
                duration_ = (int) config.get("duration").get_int64();
                double zipfian_const = config.get("zipfian_const").get_double();
                if (getenv("ZIPFIAN_CONST")) {
                    zipfian_const = atoi(getenv("ZIPFIAN_CONST")) / 100.0;
                }
                if (getenv("IDLE_USEC")) {
                    g_idle_cycles = kCpuFrequency * atoi(getenv("IDLE_USEC"));
                }
                if (getenv("INSERT_ONLY")) {
                    insert_before_execution_ = true;
                    duration_ = 1;
                }
                BindCore(0);
                if (argc >= 3) {
                    nr_threads_ = atoi(argv[1]);
                    tasks_per_thread_ = atoi(argv[2]);
                }
                index_ = new T(config, nr_threads_);
                assert(index_);
                pthread_barrier_init(&barrier_, nullptr, nr_threads_ + 1);
#ifdef DISK_DATASET
                memset(builder_, 0, kMaxThreads * sizeof(uint64_t));
#else
                for (int i = 0; i < kMaxThreads; ++i) {
                    builder_[i] = util::WorkloadBuilder::Create(dataset_, max_key_, zipfian_const);
                    assert(builder_[i]);
                }
#endif
            }

            ~BenchmarkRunner() {
                delete index_;
                // delete builder_;
                pthread_barrier_destroy(&barrier_);
            }

            int spawn() {
                if (tasks_per_thread_ <= 1 && !getenv("REPORT_LATENCY")) {
                    return spawn_one_task();
                } else {
                    return spawn_multitasks();
                }
            }

            int spawn_one_task() {
                if (read_dataset(dataset_)) {
                    return -1;
                }

                running_ = true;
                workers_.resize(nr_threads_);
                for (int i = 0; i < nr_threads_; i++) {
                    workers_[i] = std::thread(std::bind(&BenchmarkRunner<T>::worker_func, this, i));
                }

                pthread_barrier_wait(&barrier_);
                pthread_barrier_wait(&barrier_);
                clock_gettime(CLOCK_REALTIME, &start_clock_);
                sleep(duration_);
                running_ = false;
                pthread_barrier_wait(&barrier_);
                clock_gettime(CLOCK_REALTIME, &stop_clock_);
                for (int i = 0; i < nr_threads_; i++) {
                    workers_[i].join();
                }

                if (duration_) {
                    dump_result();
                }
                return 0;
            }

            void barrier(uint64_t offset, int nr_compute_nodes) {
                GlobalAddress addr(0, sizeof(SuperChunk) + offset);
                uint64_t *buf = (uint64_t *) index_->initiator()->alloc_cache(8);
                index_->initiator()->fetch_and_add(buf, addr, 1, Initiator::Option::Sync);
                int retry = 0;
                while (true) {
                    index_->initiator()->read(buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
                    if (*buf == nr_compute_nodes) {
                        break;
                    }
                    sleep(1);
                    retry++;
                    assert(retry < 60);
                }
            }

            void reset(uint64_t offset) {
                uint64_t *buf = (uint64_t *) index_->initiator()->alloc_cache(8);
                *buf = 0;
                GlobalAddress addr(0, sizeof(SuperChunk) + offset);
                index_->initiator()->write(buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
            }

            void synchronize_begin() {
                if (getenv("COMPUTE_NODES")) {
                    int nr_compute_nodes = (int) atoi(getenv("COMPUTE_NODES"));
                    if (nr_compute_nodes <= 1) return;
                    barrier(0, nr_compute_nodes);
                    reset(8);
                    reset(16);
                }
            }

            void synchronize_end() {
                if (getenv("COMPUTE_NODES")) {
                    int nr_compute_nodes = (int) atoi(getenv("COMPUTE_NODES"));
                    if (nr_compute_nodes <= 1) return;
                    reset(0);
                }
            }

            void collect() {
                if (getenv("COMPUTE_NODES")) {
                    int nr_compute_nodes = (int) atoi(getenv("COMPUTE_NODES"));
                    if (nr_compute_nodes <= 1) return;

                    uint64_t *buf = (uint64_t *) index_->initiator()->alloc_cache(8);
                    GlobalAddress addr(0, sizeof(SuperChunk) + 16);
                    index_->initiator()->fetch_and_add(buf, addr, global_ops_.load(), Initiator::Option::Sync);
                    barrier(8, nr_compute_nodes);
                    index_->initiator()->read(buf, addr, sizeof(uint64_t), Initiator::Option::Sync);
                    global_ops_.store(*buf);
                }
            }

            int spawn_multitasks() {
                if (read_dataset(dataset_)) {
                    return -1;
                }

                running_ = true;
                workers_.resize(nr_threads_);
                for (int i = 0; i < nr_threads_; i++) {
                    workers_[i] = std::thread(std::bind(&BenchmarkRunner<T>::worker_multitask_func, this, i));
                }

                pthread_barrier_wait(&barrier_);
                synchronize_begin();
                pthread_barrier_wait(&barrier_);
                clock_gettime(CLOCK_REALTIME, &start_clock_);
                sleep(duration_);
                running_ = false;
                pthread_barrier_wait(&barrier_);
                synchronize_end();
                clock_gettime(CLOCK_REALTIME, &stop_clock_);
                for (int i = 0; i < nr_threads_; i++) {
                    workers_[i].join();
                }
                collect();
                if (duration_) {
                    dump_result();
                }
                return 0;
            }

            uint64_t get_duration() {
                uint64_t total = (stop_clock_.tv_sec - start_clock_.tv_sec) * 1E9;
                total += (stop_clock_.tv_nsec - start_clock_.tv_nsec);
                return total;
            }

            uint64_t get_throughput() {
                uint64_t global_ops = global_ops_.load();
                uint64_t duration = get_duration();
                return duration > global_ops ? (global_ops * 1E9) / duration : 0;
            }

            double get_latency() {
                if (global_latency_.empty()) {
                    uint64_t global_ops = global_ops_.load();
                    uint64_t duration = get_duration();
                    return duration / (global_ops * 1.0 / nr_threads_) / 1000.0;
                } else {
                    std::sort(global_latency_.begin(), global_latency_.end());
                    return global_latency_[global_latency_.size() * 0.5] / 1000.0;
                }
            }

            double get_99latency() {
                if (global_latency_.empty()) {
                    return -1;
                } else {
                    std::sort(global_latency_.begin(), global_latency_.end());
                    return global_latency_[global_latency_.size() * 0.99] / 1000.0;
                }
            }

            void dump_result() {
                SDS_INFO("%s: workload = %s, #thread = %d, #coro_per_thread = %d, "
                         "key length = %ld, value length = %ld, max key = %ld, "
                         "throughput = %.3lf M, P50 latency = %.3lf us, P99 latency = %.3lf us\n",
                         T::Name(),
                         dataset_.c_str(),
                         nr_threads_,
                         tasks_per_thread_,
                         key_length_,
                         value_length_,
                         max_key_,
                         get_throughput() / 1000.0 / 1000.0,
                         get_latency(),
                         get_99latency());

#ifdef CONFIG_STAT
                auto res = AggregateStat();
                SDS_INFO("#average retries = %.3lf", res.total_ops ? res.retry_sum * 1.0 / res.total_ops + 1.0: -1.0);
                for (auto &e: res.hist) {
                    SDS_INFO("#retries = %d, percentage = %.3lf", e.first, e.second * 1.0 / res.total_ops);
                }
#endif

                if (dump_file_path_.empty()) {
                    return;
                }

                FILE *fout = fopen(dump_file_path_.c_str(), "a+");
                if (!fout) {
                    SDS_PERROR("fopen");
                    return;
                }

                fprintf(fout, "%s, %s, %d, %d, %ld, %ld, %ld, %.3lf, %.3lf, %.3lf",
                        T::Name(),
                        dataset_.c_str(),
                        nr_threads_,
                        tasks_per_thread_,
                        key_length_,
                        value_length_,
                        max_key_,
                        get_throughput() / 1000.0 / 1000.0,
                        get_latency(),
                        get_99latency());

#ifdef CONFIG_STAT
                fprintf(fout, ", %.3lf", res.total_ops ? res.retry_sum * 1.0 / res.total_ops + 1.0 : -1.0);
                for (auto &e: res.hist) {
                    fprintf(fout, ", %d, %.3lf", e.first, e.second * 1.0 / res.total_ops);
                }
#endif
                fprintf(fout, "\n");
                fclose(fout);
            }

        private:
            int read_dataset(const std::string &path) {
#ifdef DISK_DATASET
                struct stat st_buf;
                if (stat(path.c_str(), &st_buf)) {
                    SDS_PERROR("stat");
                    return -1;
                }

                FILE *fin = fopen(path.c_str(), "rb");
                if (!fin) {
                    SDS_PERROR("fopen");
                    return -1;
                }

                nr_records_ = st_buf.st_size / sizeof(OpRecord);
                records_ = new OpRecord[nr_records_];
                if (fread(records_, sizeof(OpRecord), nr_records_, fin) != nr_records_) {
                    SDS_PERROR("fread");
                    fclose(fin);
                    return -1;
                }

                fclose(fin);
#endif
                return 0;
            }

            std::string build_key_str(uint64_t key) {
                if (rehash_key_) {
                    key = util::FNVHash64(key);
                }
                auto str = std::to_string(key);
                if (str.size() < value_length_) {
                    return std::string(value_length_ - str.size(), '0') + str;
                } else {
                    return str.substr(0, value_length_);
                }
            }

            static inline char random_print_char() {
                thread_local std::default_random_engine engine(GetThreadID());
                thread_local std::uniform_int_distribution<int> uniform;
                return uniform(engine) % 94 + 33;
            }

            std::string build_val_str() {
                return std::string(value_length_, random_print_char());
            }

            void warm_up(int tid) {
                auto &builder = builder_[GetThreadID()];
                if (!builder || insert_before_execution_) return; 
                for (int i = 0; i < 50000; ++i) {
                    OpRecord record;
                    builder->fill_record(record);
                    auto key = build_key_str(record.key);
                    int rc;
                    if (record.type == util::INSERT) {
                        rc = index_->insert(key, build_val_str());
                    }
                    if (record.type == util::READ) {
                        std::string value;
                        rc = index_->search(key, value);
                    }
                    if (record.type == util::UPDATE) {
                        rc = index_->update(key, build_val_str());
                    }
                    if (record.type == util::SCAN) {
                        std::vector<std::string> vec;
                        rc = index_->scan(key, record.scan_len, vec);
                    }
                    if (record.type == util::READMODIFYWRITE) {
                        rc = index_->rmw(key, [](const std::string &from) -> std::string {
                            return from;
                        });
                    }
                    if (rc && record.type != util::READ) {
                        // YCSB-D may incur read miss, which is normal
                        // SDS_INFO("unexpected incident (%d): key %ld op %ld", rc, record.key, record.type);
                        // assert(0);
                    }
                }
            }

            void worker_func(int tid) {
                BindCore(tid);
                if (insert_before_execution_) {
                    for (uint64_t key = tid; key < max_key_; key += nr_threads_) {
                        int rc = index_->insert(build_key_str(key), build_val_str());
                        if (rc) {
                            SDS_INFO("unexpected value");
                            exit(EXIT_FAILURE);
                        }
                    }
                    if (tid == 0) SDS_INFO("inserted %ld keys", max_key_);
                }

                warm_up(tid);
                int start_idx = (nr_records_ / nr_threads_) * tid;
                int stop_idx = std::min(start_idx + nr_records_ / nr_threads_, nr_records_);
                int idx = start_idx;
                pthread_barrier_wait(&barrier_);
                pthread_barrier_wait(&barrier_);
                size_t local_ops = 0;
                auto &builder = builder_[GetThreadID()];
                while (running_) {
#ifdef DISK_DATASET
                    auto &record = records_[idx];
#else
                    OpRecord record;
                    builder->fill_record(record);
#endif
                    auto key = build_key_str(record.key);
                    int rc;
                    if (record.type == util::INSERT) {
                        rc = index_->insert(key, build_val_str());
                    }
                    if (record.type == util::READ) {
                        std::string value;
                        rc = index_->search(key, value);
                    }
                    if (record.type == util::UPDATE) {
                        rc = index_->update(key, build_val_str());
                    }
                    if (record.type == util::SCAN) {
                        std::vector<std::string> vec;
                        rc = index_->scan(key, record.scan_len, vec);
                    }
                    if (record.type == util::READMODIFYWRITE) {
                        rc = index_->rmw(key, [](const std::string &from) -> std::string {
                            return from;
                        });
                    }
                    if (rc && record.type != util::READ) {
                        // YCSB-D may incur read miss, which is normal
                        // SDS_INFO("unexpected incident (%d): key %ld op %ld", rc, record.key, record.type);
                        // assert(0);
                    }
                    local_ops++;
                    idx++;
                    if (idx == stop_idx) idx = start_idx;
                }
                pthread_barrier_wait(&barrier_);
                global_ops_.fetch_add(local_ops);
            }

            void task_func(int tid, int total_tasks, size_t *local_ops, int *running_tasks) {
                auto &builder = builder_[GetThreadID()];
                if (getenv("REPORT_LATENCY")) {
                    int start_idx = (nr_records_ / total_tasks) * tid;
                    int stop_idx = std::min(start_idx + nr_records_ / total_tasks, nr_records_);
                    int idx = start_idx;
                    std::vector<uint64_t> local_latency;
                    while (running_) {
#ifdef DISK_DATASET
                        auto &record = records_[idx];
#else
                        OpRecord record;
                        builder->fill_record(record);
#endif
                        auto key = build_key_str(record.key);
                        int rc;
                        timespec start, end;
                        clock_gettime(CLOCK_REALTIME, &start);
                        if (record.type == util::INSERT) {
                            rc = index_->insert(key, build_val_str());
                        }
                        if (record.type == util::READ) {
                            std::string value;
                            rc = index_->search(key, value);
                        }
                        if (record.type == util::UPDATE) {
                            rc = index_->update(key, build_val_str());
                        }
                        if (record.type == util::SCAN) {
                            std::vector<std::string> vec;
                            rc = index_->scan(key, record.scan_len, vec);
                        }
                        if (record.type == util::READMODIFYWRITE) {
                            rc = index_->rmw(key, [](const std::string &from) -> std::string {
                                return from;
                            });
                        }
                        if (rc) {
                            // SDS_INFO("unexpected incident (%d): key %ld op %ld", rc, record.key, record.type);
                        }
                        clock_gettime(CLOCK_REALTIME, &end);
                        local_latency.push_back((end.tv_sec - start.tv_sec) * 1E9 + (end.tv_nsec - start.tv_nsec));
                        (*local_ops)++;
                        idx++;
                        if (idx == stop_idx) idx = start_idx;
                        IdleExecution();
                    }
                    global_latency_lock_.lock();
                    for (auto &entry: local_latency) {
                        global_latency_.push_back(entry);
                    }
                    global_latency_lock_.unlock();
                } else {
                    int start_idx = (nr_records_ / total_tasks) * tid;
                    int stop_idx = std::min(start_idx + nr_records_ / total_tasks, nr_records_);
                    int idx = start_idx;
                    while (running_) {
#ifdef DISK_DATASET
                        auto &record = records_[idx];
#else
                        OpRecord record;
                        builder->fill_record(record);
#endif
                        auto key = build_key_str(record.key);
                        int rc;
                        if (record.type == util::INSERT) {
                            rc = index_->insert(key, build_val_str());
                        }
                        if (record.type == util::READ) {
                            std::string value;
                            rc = index_->search(key, value);
                        }
                        if (record.type == util::UPDATE) {
                            rc = index_->update(key, build_val_str());
                        }
                        if (record.type == util::SCAN) {
                            std::vector<std::string> vec;
                            rc = index_->scan(key, record.scan_len, vec);
                        }
                        if (record.type == util::READMODIFYWRITE) {
                            rc = index_->rmw(key, [](const std::string &from) -> std::string {
                                return from;
                            });
                        }
                        if (rc) {
                            // SDS_INFO("unexpected incident (%d): key %ld op %ld", rc, record.key, record.type);
                        }
                        (*local_ops)++;
                        idx++;
                        if (idx == stop_idx) idx = start_idx;
                    }
                }
                (*running_tasks)--;
            }

            void worker_multitask_func(int tid) {
                BindCore(tid);
                if (insert_before_execution_) {
                    for (uint64_t key = tid; key < max_key_; key += nr_threads_) {
                        int rc = index_->insert(build_key_str(key), build_val_str());
                        if (rc) {
                            SDS_INFO("unexpected value");
                            exit(EXIT_FAILURE);
                        }
                    }
                    if (tid == 0) SDS_INFO("inserted %ld keys", max_key_);
                }

                warm_up(tid);
                size_t local_ops = 0;
                ResetStat();
                TaskPool::Enable();
                auto &task_pool = TaskPool::Get();
                int running_tasks = tasks_per_thread_;
                task_pool.spawn(index_->get_poll_task(running_tasks));
                for (int i = 0; i < tasks_per_thread_; ++i) {
                    task_pool.spawn(std::bind(&BenchmarkRunner<T>::task_func,
                                              this,
                                              tid * tasks_per_thread_ + i,
                                              nr_threads_ * tasks_per_thread_,
                                              &local_ops,
                                              &running_tasks));
                }
                pthread_barrier_wait(&barrier_);
                pthread_barrier_wait(&barrier_);
                while (!task_pool.empty()) {
                    YieldTask();
                }
                pthread_barrier_wait(&barrier_);
                global_ops_.fetch_add(local_ops);
            }

        private:
            JsonConfig config_;
            T *index_;
            std::vector<std::thread> workers_;

            int nic_numa_node_;
            int cpu_nodes_;
            int cores_per_cpu_;
            int tasks_per_thread_;
            int nr_threads_;
            int duration_;
            bool insert_before_execution_;
            bool rehash_key_;
            size_t max_key_;
            size_t key_length_;
            size_t value_length_;
            std::string dump_file_path_;
            std::string dataset_;

            timespec start_clock_, stop_clock_;
            pthread_barrier_t barrier_;
            std::atomic<uint64_t> global_ops_;

            volatile bool running_;

            std::vector<uint64_t> global_latency_;
            std::mutex global_latency_lock_;

            util::WorkloadBuilder *builder_[kMaxThreads];
            OpRecord *records_;
            size_t nr_records_;
        };
    }
}

#endif //SDS_BENCHMARK_H

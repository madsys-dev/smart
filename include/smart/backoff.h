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

#ifndef SDS_BACKOFF_H
#define SDS_BACKOFF_H

#include "task.h"
#include "thread.h"

namespace sds {
    class Backoff {
    public:
        const static uint64_t kEpochInterval = 2400 * 1000 * 20; // 1ms

    public:
        constexpr const static double kDefaultLowWaterMark = 0.1;
        constexpr const static double kDefaultHighWaterMark = 0.5;
        const static int kDefaultMaxCredit = kMaxTasksPerThread;
        const static uint64_t kInitialBackoffCycles = 4096;
        const static uint64_t kMaxBackoffCycles = kInitialBackoffCycles << 10;

        Backoff() : enabled_(true),
                    credit_(kDefaultMaxCredit),
                    max_credit_(kDefaultMaxCredit),
                    retry_count_(0), task_count_(0),
                    low_water_mark_(kDefaultLowWaterMark),
                    high_water_mark_(kDefaultHighWaterMark),
                    last_tsc_(rdtsc()), task_throttling_(true), auto_adjust_(true) {
            if (getenv("DISABLE_CONFLICT_AVOIDANCE")) {
                enabled_ = false;
            }
            if (getenv("DISABLE_CORO_THROT")) {
                task_throttling_ = false;
            }
            max_backoff_cycles_ = kInitialBackoffCycles;
            if (getenv("MAX_BACKOFF_CYCLE_LOG2")) {
                auto_adjust_ = false;
                long v = atoi(getenv("MAX_BACKOFF_CYCLE_LOG2"));
                if (v >= 0) {
                    max_backoff_cycles_ = kInitialBackoffCycles << v;
                } else {
                    max_backoff_cycles_ = kInitialBackoffCycles << 20;
                }
            }
        }

        void begin_task() {
            int task_id = GetTaskID();
            backoff_[task_id] = kInitialBackoffCycles;
            if (enabled_ && task_throttling_) {
                task_list_.push(task_id);
                while (task_list_.front() != task_id || credit_ <= 0) {
                    WaitTask();
                }
                task_list_.pop();
                credit_--;
            }
            compiler_fence();
        }

        void end_task() {
            RecordOps();
            compiler_fence();
            task_count_++;
            if (enabled_ && task_throttling_) {
                credit_++;
                if (credit_ > 0 && !task_list_.empty()) {
                    NotifyTask(task_list_.front());
                }
            }
        }

        void retry_task(bool suspend = true) {
            RecordRetry();
            retry_count_++;
            if (!enabled_) {
                return;
            }
            if (__glibc_unlikely(rdtsc() - last_tsc_ > kEpochInterval)) {
                if (task_throttling_) {
                    update_credit();
                } else if (auto_adjust_) {
                    update_backoff();
                }
                last_tsc_ = rdtsc();
                retry_count_ = 0;
                task_count_ = 0;
            }
            if (__glibc_likely(suspend)) {
                uint64_t start_cycle = rdtsc();
                int task_id = GetTaskID();
                uint64_t backoff = backoff_[task_id] + GetRandInt(&seed_) % kInitialBackoffCycles;
                while (rdtsc() < start_cycle + backoff) {
                    YieldTask();
                    _mm_pause();
                    compiler_fence();
                }
                backoff_[task_id] = std::min(backoff_[task_id] * 2, max_backoff_cycles_);
            }
        }

        __attribute_noinline__
        void update_credit() {
            if (retry_count_ + task_count_ == 0) return;
            double retry_rate = retry_count_ * 1.0 / (retry_count_ + task_count_);
            if (retry_rate < low_water_mark_) {
                int new_max_credit = std::min(max_credit_ * 2, (int) kMaxTasksPerThread);
                if (new_max_credit == max_credit_ && auto_adjust_) {
                    max_backoff_cycles_ = std::max(max_backoff_cycles_ / 2, kInitialBackoffCycles);
                } else {
                    credit_ += new_max_credit - max_credit_;
                    max_credit_ = new_max_credit;
                }
            } else if (retry_rate > high_water_mark_) {
                int new_max_credit = std::max(max_credit_ / 2, 1);
                if (new_max_credit == max_credit_ && auto_adjust_) {
                    max_backoff_cycles_ = std::min(max_backoff_cycles_ * 2, kMaxBackoffCycles);
                } else {
                    credit_ += new_max_credit - max_credit_;
                    max_credit_ = new_max_credit;
                }
            }
        }

        __attribute_noinline__
        void update_backoff() {
            if (retry_count_ + task_count_ == 0) return;
            double retry_rate = retry_count_ * 1.0 / (retry_count_ + task_count_);
            if (retry_rate < low_water_mark_) {
                max_backoff_cycles_ = std::max(max_backoff_cycles_ / 2, kInitialBackoffCycles);
            } else if (retry_rate > high_water_mark_) {
                max_backoff_cycles_ = std::min(max_backoff_cycles_ * 2, kMaxBackoffCycles);
            }
        }

    private:
        struct FixedQueue {
            FixedQueue() : head(0), tail(0) {}

            void push(int task_id) {
                elem[head] = task_id;
                head++;
                if (head == kMaxTasksPerThread) {
                    head = 0;
                }
            }

            bool empty() const {
                return head == tail;
            }

            void pop() {
                assert(!empty());
                tail++;
                if (tail == kMaxTasksPerThread) {
                    tail = 0;
                }
            }

            int front() {
                assert(!empty());
                return elem[tail];
            }

            int head, tail;
            int elem[kMaxTasksPerThread];
        };

        static uint32_t GetRandInt(uint64_t* seed) {
            *seed = *seed * 1103515245 + 12345;
            return (uint32_t)(*seed >> 32);
        }

        static inline int GetComputeNodes() {
            static int g_compute_nodes = 0;
            if (g_compute_nodes >= 1)
                return g_compute_nodes;
            if (getenv("COMPUTE_NODES")) {
                g_compute_nodes = atoi(getenv("COMPUTE_NODES"));
            } else {
                g_compute_nodes = 1;
            }
            return g_compute_nodes;
        }

    private:
        int credit_;
        int max_credit_;
        uint64_t last_tsc_;
        int retry_count_;
        int task_count_;
        bool enabled_;
        bool task_throttling_;
        FixedQueue task_list_;
        uint64_t seed_;
        uint64_t backoff_[kMaxTasksPerThread];
        const double low_water_mark_;
        const double high_water_mark_;
        uint64_t max_backoff_cycles_;
        bool auto_adjust_;
    };

    class BackoffGuard {
    public:
        BackoffGuard(Backoff &backoff) :
                backoff_(backoff) {
            backoff_.begin_task();
        }
        ~BackoffGuard() {
            backoff_.end_task();
        }
        void retry_task(bool suspend = true) {
            backoff_.retry_task(suspend);
        }
    private:
        Backoff &backoff_;
    };
}

#endif //SDS_BACKOFF_H

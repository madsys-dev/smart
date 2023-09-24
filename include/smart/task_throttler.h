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

#ifndef SDS_TASK_THROTTLER_H
#define SDS_TASK_THROTTLER_H

#include "task.h"
#include "thread.h"

namespace sds {
    class TaskThrottler {
        const static uint64_t kEpochInterval = 2400 * 1000 * 20; // 1ms

    public:
        constexpr const static double kDefaultLowWaterMark = 0.1;
        constexpr const static double kDefaultHighWaterMark = 0.5;
        const static int kDefaultMaxCredit = kMaxTasksPerThread;

        TaskThrottler(double low_water_mark = kDefaultLowWaterMark, double high_water_mark = kDefaultHighWaterMark) :
                enabled_(true), credit_(kDefaultMaxCredit), max_credit_(kDefaultMaxCredit),
                retry_count_(0), task_count_(0), low_water_mark_(low_water_mark), high_water_mark_(high_water_mark),
                last_tsc_(rdtsc()), static_credit_(false) {
            if (getenv("DISABLE_CONFLICT_AVOIDANCE")) {
                enabled_ = false;
            }
            if (getenv("DISABLE_CORO_THROT")) {
                static_credit_ = true;
            }
        }
        
        bool enabled() const {
            return enabled_;
        }

        void begin_task() {
            if (!enabled_) {
                return;
            }
            int task_id = GetTaskID();
            task_list_.push(task_id);
            while (task_list_.front() != task_id || credit_ <= 0) {
                WaitTask();
            }
            task_list_.pop();
            credit_--;
            compiler_fence();
        }

        void end_task() {
            RecordOps();
            compiler_fence();
            if (!enabled_) {
                return;
            }
            task_count_++;
            credit_++;
            if (credit_ > 0 && !task_list_.empty()) {
                NotifyTask(task_list_.front());
            }
        }

        void retry_task() {
            if (!enabled_) {
                return;
            }
            retry_count_++;
            if (__glibc_unlikely(!static_credit_ && rdtsc() - last_tsc_ > kEpochInterval)) {
                update_credit();
            }
        }

        __attribute_noinline__
        void update_credit() {
            double retry_rate = retry_count_ * 1.0 / (retry_count_ + task_count_);
            if (retry_rate < low_water_mark_) {
                int new_max_credit = std::min(max_credit_ * 2, (int) kMaxTasksPerThread);
                if (new_max_credit != max_credit_ && GetThreadID() == 2) {
                    // SDS_INFO("update max credit: %d, retry_rate %.3lf", new_max_credit, retry_rate);
                }
                credit_ += new_max_credit - max_credit_;
                max_credit_ = new_max_credit;
            } else if (retry_rate > high_water_mark_) {
                int new_max_credit = std::max(max_credit_ / 2, 1);
                if (new_max_credit != max_credit_ && GetThreadID() == 2) {
                    // SDS_INFO("update max credit: %d, retry_rate %.3lf", new_max_credit, retry_rate);
                }
                credit_ += new_max_credit - max_credit_;
                max_credit_ = new_max_credit;
            }
            last_tsc_ = rdtsc();
            retry_count_ = 0;
            task_count_ = 0;
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

    private:
        int credit_;
        int max_credit_;
        uint64_t last_tsc_;
        int retry_count_;
        int task_count_;
        bool enabled_;
        bool static_credit_;
        FixedQueue task_list_;
        const double low_water_mark_;
        const double high_water_mark_;
    };

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

    template<int INIT_BACKOFF_CYCLES, int MAX_EXP>
    class TaskThrottlerGuardImpl {
        const uint64_t kInitialBackoffCycles = INIT_BACKOFF_CYCLES;
        uint64_t kMaxBackoffCycles = kInitialBackoffCycles << std::min(10, MAX_EXP - 1 + GetComputeNodes());

    public:
        TaskThrottlerGuardImpl(TaskThrottler &throttler) :
                throttler_(throttler),
                seed_(rdtsc()),
                backoff_(kInitialBackoffCycles) {
            if (getenv("MAX_BACKOFF_CYCLE_LOG2")) {
                long v = atoi(getenv("MAX_BACKOFF_CYCLE_LOG2"));
                if (v >= 0)
                    kMaxBackoffCycles = kInitialBackoffCycles << v;
                else
                    kMaxBackoffCycles = UINT64_MAX;
            }
            throttler.begin_task();
        }

        ~TaskThrottlerGuardImpl() {
            throttler_.end_task();
        }

        void retry_task(bool do_pause = true) {
            RecordRetry();
            if (!throttler_.enabled()) {
                return;
            }
            throttler_.retry_task();
            if (!do_pause) {
                return;
            }
            uint64_t start_cycle = rdtsc();
            uint64_t backoff = backoff_ + fast_rand(&seed_) % kInitialBackoffCycles;
            while (rdtsc() < start_cycle + backoff) {
                YieldTask();
                compiler_fence();
            }
            backoff_ = std::min(backoff_ * 2, kMaxBackoffCycles);
        }

    private:
        static uint32_t fast_rand(uint64_t* seed) {
            *seed = *seed * 1103515245 + 12345;
            return (uint32_t)(*seed >> 32);
        }

    private:
        TaskThrottler &throttler_;
        uint64_t seed_;
        uint64_t backoff_;
    };

    using TaskThrottlerGuard = TaskThrottlerGuardImpl<4096, 6>;
}

#endif //SDS_TASK_THROTTLER_H

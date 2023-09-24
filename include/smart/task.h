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

#ifndef SDS_TASK_H
#define SDS_TASK_H

#include <functional>
#include <cstdint>
#include <atomic>
#include "common.h"
#include "thread.h"
#include "util/fcontext.h"

namespace sds {
    extern thread_local bool tl_task_pool_enabled;

    class TaskPool {
    public:
        const static size_t kTaskStackSize = 64 * 1024;
        const static size_t kForceUnblockInterval = 1024;

    private:
        struct StackContext {
            void *sp;
            size_t size;
        };

        struct Task {
            fcontext_t fctx;
            StackContext sc;
            Task *prev;
            Task *next;
            char init;
            char blocked;
            char exit;
            int id;
            std::function<void()> func;
        };

        struct Queue {
            Task head, tail;
        };

        static transfer_t g_empty_transfer;

    private:
        Task *active;
        int nr_running;
        int nr_blocking;
        int max_task_id;
        int nr_schedule;
        Queue running;
        Queue blocking;
        Task task_list[kMaxTasksPerThread];

    public:
        TaskPool() {
            auto &main = task_list[0];
            main.id = 0;
            main.exit = 0;
            main.init = 1;
            main.blocked = 0;
            active = &main;
            nr_running = 0;
            nr_blocking = 0;
            max_task_id = 0;
            for (int i = 1; i < kMaxTasksPerThread; ++i) {
                auto &task = task_list[i];
                task.sc.size = 0;
                task.id = i;
                task.init = 0;
            }
            init_queue(&running);
            init_queue(&blocking);
            enqueue(&running, &main);
        }

        ~TaskPool() {
            for (int i = 1; i < kMaxTasksPerThread; ++i) {
                stack_context_free(&task_list[i].sc);
            }
        }

        TaskPool(const TaskPool &) = delete;

        TaskPool &operator=(const TaskPool &) = delete;

        static TaskPool &Get() {
            assert(tl_task_pool_enabled);
            thread_local TaskPool *g_task_pool = new TaskPool();
            return *g_task_pool;
        }

        static void Enable() {
            assert(!tl_task_pool_enabled);
            tl_task_pool_enabled = true;
        }

        static bool IsEnabled() {
            return tl_task_pool_enabled;
        }

        void spawn(const std::function<void()> &func, size_t stack_size = kTaskStackSize) {
            Task *task = nullptr;
            while (true) {
                for (int i = 1; i < kMaxTasksPerThread; ++i) {
                    if (!task_list[i].init) {
                        max_task_id = std::max(max_task_id, i);
                        task = &task_list[i];
                        break;
                    }
                }
                if (__glibc_likely(task != nullptr)) break;
                schedule(true);
            }
            nr_running++;
            task->func = func;
            task->init = 1;
            task->blocked = 0;
            task->exit = 0;
            if (task->sc.size != stack_size) {
                stack_context_free(&task->sc);
                task->sc = stack_context_alloc(stack_size);
            }
            task->fctx = make_fcontext(task->sc.sp, task->sc.size, &fcontext_entry);
            task->fctx = jump_fcontext(task->fctx, task).fctx;
            enqueue(&running, task);
        }

        bool empty() const {
            return nr_running == 0;
        }

        void yield() {
            schedule(true);
        }

        void wait() {
            auto task = active;
            if (task->id != 0) {
                task->blocked = 1;
                enqueue(&blocking, task);
                nr_blocking++;
                schedule(false);
            }
        }

        void notify_all() {
            if (nr_blocking) {
                Task *task = dequeue(&blocking);
                while (task) {
                    task->blocked = 0;
                    enqueue(&running, task);
                    task = dequeue(&blocking);
                }
                nr_blocking = 0;
            }
        }

        void notify() {
            if (nr_blocking) {
                Task *task = dequeue(&blocking);
                assert(task);
                task->blocked = 0;
                enqueue(&running, task);
                nr_blocking--;
            }
        }

        void notify(int task_id) {
            Task *task = &task_list[task_id];
            if (nr_blocking && task->blocked) {
                detach(task);
                task->blocked = 0;
                enqueue(&running, task);
                nr_blocking--;
            }
        }

        int current() const {
            return active->id;
        }

    private:
        void *operator new(size_t size) {
            uint64_t *ptr = (uint64_t *) numa_alloc_local(size);
            ptr[0] = size;
            return &ptr[1];
        }

        void operator delete(void *p) {
            uint64_t *ptr = (uint64_t *) p;
            numa_free(&ptr[-1], ptr[-1]);
        }

    private:
        void schedule(bool reuse) {
            nr_schedule++;
            if (__glibc_unlikely(nr_schedule > kForceUnblockInterval)) {
                nr_schedule = 0;
                notify_all();
            }
            auto task = dequeue(&running);
            auto prev = active;
            if (__glibc_unlikely(prev == task)) {
                return;
            }
            if (reuse) {
                enqueue(&running, prev);
            }
            active = task;
            ontop_fcontext(task->fctx, prev, &fcontext_ontop);
        }

    private:
        static struct StackContext stack_context_alloc(size_t stack_size) {
            StackContext sc{};
            sc.size = stack_size;
            char *ptr = (char *) numa_alloc_local(sc.size);
            if (!ptr || (uint64_t) ptr & 4095) {
                SDS_PERROR("numa_alloc_local");
                exit(EXIT_FAILURE);
            }
            sc.sp = ptr + sc.size;
            return sc;
        }

        static void stack_context_free(StackContext *sc) {
            if (sc->size) {
                char *ptr = (char *) sc->sp - sc->size;
                numa_free(ptr, sc->size);
            }
        }

        static inline void init_queue(Queue *queue) {
            queue->head.prev = &queue->tail;
            queue->head.next = nullptr;
            queue->tail.prev = nullptr;
            queue->tail.next = &queue->head;
        }

        static inline void enqueue(Queue *queue, Task *new_node) {
            auto *prev = queue->head.prev;
            prev->next = new_node;
            new_node->prev = prev;
            new_node->next = &queue->head;
            queue->head.prev = new_node;
        }

        static inline Task *dequeue(Queue *queue) {
            auto task = queue->tail.next;
            if (task == &queue->head) {
                return nullptr;
            }
            queue->tail.next = task->next;
            task->next->prev = &queue->tail;
            task->next = task->prev = nullptr;
            return task;
        }

        static inline void detach(Task *task) {
            auto prev = task->prev;
            auto next = task->next;
            prev->next = next;
            next->prev = prev;
            task->next = task->prev = nullptr;
        }

        static transfer_t fcontext_ontop(transfer_t t) {
            auto prev = (Task *) t.data;
            if (prev->exit) {
                prev->init = 0;
            } else {
                prev->fctx = t.fctx;
            }
            return g_empty_transfer;
        }

        static void fcontext_entry(transfer_t t) {
            auto task = (Task *) t.data;
            jump_fcontext(t.fctx, nullptr);
            task->func();
            task->exit = 1;
            TaskPool::Get().nr_running--;
            TaskPool::Get().schedule(false);
        }
    };

    static inline int GetTaskID() {
        if (tl_task_pool_enabled) {
            return TaskPool::Get().current();
        } else {
            return 0;
        }
    }

    static inline void YieldTask() {
        sfence();
        if (tl_task_pool_enabled) {
            TaskPool::Get().yield();
        }
        sfence();
    }

    static inline void WaitTask() {
        sfence();
        if (tl_task_pool_enabled) {
            TaskPool::Get().wait();
        }
        sfence();
    }

    static inline void NotifyTask() {
        if (tl_task_pool_enabled) {
            TaskPool::Get().notify();
        }
    }

    static inline void NotifyTask(int task_id) {
        if (tl_task_pool_enabled) {
            TaskPool::Get().notify(task_id);
        }
    }

    static inline void NotifyAllTasks() {
        if (tl_task_pool_enabled) {
            TaskPool::Get().notify_all();
        }
    }

    class TaskLockGuard {
    public:
        TaskLockGuard(bool &locked) : locked_(locked) {
            while (locked_) {
                WaitTask();
            }
            locked_ = true;
        }

        ~TaskLockGuard() {
            locked_ = false;
            NotifyTask();
        }

    private:
        bool &locked_;
    };
}

#endif //SDS_TASK_H

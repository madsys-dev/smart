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

#ifndef SDS_COMMON_H
#define SDS_COMMON_H

#include <string>
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <immintrin.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#define ALWAYS_INLINE inline __attribute__((always_inline))
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#define ROOT_DIR ".."

#define RED "\033[1;31m"
#define DEFAULT "\033[0m"

#ifndef NDEBUG
#define SDS_INFO(format...)                                                                     \
        do {                                                                                    \
            char __buf[1024];                                                                   \
            snprintf(__buf, 1024, format);                                                      \
            fprintf(stderr, "[I] %s:%d: %s(): %s\n", __FILE__, __LINE__, __func__, __buf);      \
        } while (0)
#else
#define SDS_INFO(format...)
#endif

#define SDS_WARN(format...)                                                                     \
        do {                                                                                    \
            char __buf[1024];                                                                   \
            snprintf(__buf, 1024, format);                                                      \
            fprintf(stderr, RED "[W] %s:%d: %s(): %s\n" DEFAULT, __FILE__, __LINE__, __func__, __buf);    \
        } while (0)

#define SDS_PERROR(str)                                                                         \
        do {                                                                                    \
            fprintf(stderr, RED "[E] %s:%d: %s(): %s: %s\n" DEFAULT, __FILE__, __LINE__, __func__, str, strerror(errno)); \
        } while (0)

namespace sds {
    enum MemoryRegion {
        RPC_REGION = 0,
        DEVICE_MEMORY_REGION = 1,
        CACHE_REGION = 2,
        MEMORY_POOL_REGION = 3,
    };

    const static size_t kMaxThreads = 128;
    const static size_t kCacheLineSize = 64;
    const static size_t kMegaBytes = 1ull << 20;
    const static size_t kHugePageSize = 2ull << 20;
    const static size_t kDeviceMemorySize = 256 * 1024;
    const static size_t kNumOfLock = 32768;
    const static size_t kChunkShift = 14; // 16K
    const static size_t kChunkSize = 1ull << kChunkShift;
    const static size_t kMaxTasksPerThread = 64;

    template<typename T>
    static inline bool cmpxchg64(volatile T *addr, T old_val, T new_val) {
        static_assert(sizeof(T) == sizeof(uint64_t), "");
        return __sync_bool_compare_and_swap((uint64_t *) addr, (uint64_t) old_val,
                                            (uint64_t) new_val);
    }

    template<typename T>
    static inline void xchg64(T *addr, T new_val) {
        static_assert(sizeof(T) == sizeof(uint64_t), "");
        __sync_lock_test_and_set((uint64_t *) addr, (uint64_t) new_val);
    }

    static inline void mfence() {
        asm volatile("mfence" : : : "memory");
    }

    static inline void sfence() {
        asm volatile("sfence" : : : "memory");
    }

    static inline void lfence() {
        asm volatile("lfence" : : : "memory");
    }

    static inline void compiler_fence() {
        asm volatile("" : : : "memory");
    }

    static inline uint64_t rdtsc() {
        uint64_t rax, rdx;
        asm volatile ("rdtsc" : "=a" (rax), "=d" (rdx) : : );
        return (rdx << 32) + rax;
    }

    static inline uint64_t rdtscp(uint32_t &aux) {
        uint64_t rax, rdx;
        asm volatile ("rdtscp" : "=a" (rax), "=d" (rdx), "=c" (aux) : : );
        return (rdx << 32) + rax;
    }

    static inline void clwb(volatile void *p) {
        asm volatile(".byte 0x66; xsaveopt %0" : "+m"(p));
    }

    static inline void prefetch(const void *ptr) {
        _mm_prefetch(ptr, _MM_HINT_T2);
    }

    static inline void *mmap_huge_page(size_t capacity) {
        void *addr = (char *) mmap(nullptr, capacity, PROT_READ | PROT_WRITE,
                                   MAP_ANON | MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB,
                                   -1, 0);
        if (addr == MAP_FAILED) {
            SDS_PERROR("mmap");
            SDS_INFO("Please check if you have enough huge pages to be allocated");
            exit(EXIT_FAILURE);
        }
        return addr;
    }

    static inline void *mapping_memory(const std::string &dev_path, size_t capacity) {
        if (dev_path.empty()) {
            return mmap_huge_page(capacity);
        } else {
            int fd = open(dev_path.c_str(), O_RDWR);
            if (fd < 0) {
                SDS_PERROR("open");
                exit(EXIT_FAILURE);
            }
            struct stat st_buf;
            int rc = fstat(fd, &st_buf);
            if (rc || !S_ISCHR(st_buf.st_mode)) {
                SDS_WARN("not a character file");
                exit(EXIT_FAILURE);
            }
            void *mmap_addr = mmap(nullptr, capacity, PROT_READ | PROT_WRITE,
                                   MAP_SHARED_VALIDATE | MAP_SYNC, fd, 0);
            if (mmap_addr == MAP_FAILED) {
                SDS_PERROR("mmap");
                exit(EXIT_FAILURE);
            }
            memset(mmap_addr, 0, std::min(16 * kMegaBytes, capacity));
            close(fd);
            return mmap_addr;
        }
    }

    static inline std::string GetHostName() {
        const static int kLength = 128;
        char buf[kLength];
        gethostname(buf, kLength);
        return std::string(buf, kLength);
    }
}

#endif //SDS_COMMON_H

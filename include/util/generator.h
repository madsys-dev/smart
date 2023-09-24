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

#ifndef SDS_GENERATOR_H
#define SDS_GENERATOR_H
#include <cmath>
#include <mutex>
#include <atomic>
#include <cstdlib>
#include <cstdint>
#include <cassert>
#include <random>

#include "smart/thread.h"
#include "zipf.h"
#include "util/murmur.h"

namespace sds {
    namespace util {
        static inline uint32_t GetSeed() {
            auto hostname = GetHostName();
            return MurmurHash3_x86_32(hostname.c_str(), hostname.length(), 0x12345678) * kMaxThreads + GetThreadID();
        }

        inline double RandomDouble(double min = 0.0, double max = 1.0) {
            thread_local std::default_random_engine generator(GetSeed());
            thread_local std::uniform_real_distribution<double> uniform(min, max);
            return uniform(generator);
        }

        inline char RandomPrintChar() {
            return rand() % 94 + 33;
        }

        const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
        const uint64_t kFNVPrime64 = 1099511628211;

        inline uint64_t FNVHash64(uint64_t val) {
            uint64_t hash = kFNVOffsetBasis64;

            for (int i = 0; i < 8; i++) {
                uint64_t octet = val & 0x00ff;
                val = val >> 8;

                hash = hash ^ octet;
                hash = hash * kFNVPrime64;
            }
            return hash;
        }

        class Generator {
        public:
            virtual uint64_t next() = 0;

            virtual uint64_t last() = 0;

            virtual ~Generator() {}
        };

        class ZipfianGenerator {
        public:
            static const uint64_t kMaxNumItems = (UINT64_MAX >> 24);

            ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const) :
                    num_items_(max - min + 1), base_(min) {
                assert(num_items_ >= 2 && num_items_ < kMaxNumItems);
                mehcached_zipf_init(&state_, num_items_, zipfian_const, (rdtsc() * kMaxTasksPerThread + GetThreadID()) % (UINT32_MAX));
                next(num_items_);
            }

            ZipfianGenerator(uint64_t num_items, double zipfian_const) :
                    ZipfianGenerator(0, num_items - 1, zipfian_const) {}

            uint64_t next(uint64_t num) {
                return last_value_ = base_ + mehcached_zipf_next(&state_) % num;
            }

            uint64_t next() { return next(num_items_); }

            uint64_t last() { return last_value_; }

        private:
            struct zipf_gen_state state_;
            uint64_t num_items_;
            uint64_t base_;
            uint64_t last_value_;
        };

        class UniformGenerator : public Generator {
        public:
            // Both min and max are inclusive
            UniformGenerator(uint64_t min, uint64_t max) {
                dist = std::uniform_int_distribution<uint64_t>(min, max);
                next();
            }

            uint64_t next() {
                thread_local std::mt19937_64 generator(GetSeed());
                return last_int = dist(generator);
            }

            uint64_t last() {
                return last_int;
            }

        private:
            std::uniform_int_distribution<uint64_t> dist;
            uint64_t last_int;
        };

        class CounterGenerator : public Generator {
        public:
            CounterGenerator(uint64_t start) : last_int_(0) { counter() = start; }

            uint64_t next() { return last_int_ = counter().fetch_add(1); }

            uint64_t last() {
                if (last_int_ == 0) {
                    return counter().load() - 1;
                } else {
                    return last_int_;
                }
            }

            void set(uint64_t start) { counter().store(start); }

        private:
            static std::atomic<uint64_t> &counter() {
                static std::atomic<uint64_t> val;
                return val;
            };

            uint64_t last_int_;
        };

        class SkewedLatestGenerator : public Generator {
        public:
            SkewedLatestGenerator(CounterGenerator &counter, double zipfian_const) :
                    basis_(counter), zipfian_(basis_.last(), zipfian_const) {
                next();
            }

            uint64_t next() {
                uint64_t max = basis_.last();
                return last_int_ = max - zipfian_.next(max);
            }

            uint64_t last() { return last_int_; }

        private:
            CounterGenerator &basis_;
            ZipfianGenerator zipfian_;
            uint64_t last_int_;
        };

        class ScrambledZipfianGenerator : public Generator {
        public:
            ScrambledZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const) :
                    base_(min), num_items_(max - min + 1),
                    generator_(min, max, zipfian_const) {}

            ScrambledZipfianGenerator(uint64_t num_items, double zipfian_const) :
                    ScrambledZipfianGenerator(0, num_items - 1, zipfian_const) {}

            uint64_t next() {
                return scramble(generator_.next());
            }

            uint64_t last() {
                return scramble(generator_.last());
            }

        private:
            const uint64_t base_;
            const uint64_t num_items_;
            ZipfianGenerator generator_;

            uint64_t scramble(uint64_t value) const {
                return base_ + FNVHash64(value) % num_items_;
            }
        };

        template<typename Value>
        class DiscreteGenerator {
        public:
            DiscreteGenerator() : sum_(0) {}

            void addValue(Value value, double weight) {
                if (values_.empty()) {
                    last_ = value;
                }
                values_.push_back(std::make_pair(value, weight));
                sum_ += weight;
            }

            Value next() {
                double chooser = RandomDouble();
                for (auto p = values_.cbegin(); p != values_.cend(); ++p) {
                    if (chooser < p->second / sum_) {
                        return last_ = p->first;
                    }
                    chooser -= p->second / sum_;
                }
                assert(false);
                return last_;
            }

            Value last() { return last_; }

        private:
            std::vector<std::pair<Value, double>> values_;
            double sum_;
            Value last_;
        };
    }
}


#endif //SDS_GENERATOR_H

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

#ifndef SDS_YCSB_H
#define SDS_YCSB_H

#include "generator.h"

namespace sds {
    namespace util {
        enum OpType {
            INSERT = 0, READ, UPDATE, SCAN, READMODIFYWRITE
        };

        struct OpRecord {
            uint64_t type: 4;
            uint64_t scan_len: 8;
            uint64_t key: 52;
        };

        class WorkloadBuilder {
        public:
            WorkloadBuilder(size_t initial_elements, int *percentage, const std::string &key_dist, double zipfian_const)
                    : insert_key(0), scan_len(1, 100) {
                for (int i = 0; i <= READMODIFYWRITE; ++i) {
                    op_type.addValue((OpType) i, percentage[i]);
                }
                insert_key.set(initial_elements);
                if (key_dist == "zipfian") {
                    int new_keys = (int) (initial_elements * percentage[INSERT] / 100 * 2); // a fudge factor
                    query_key = new ScrambledZipfianGenerator(initial_elements + new_keys, zipfian_const);
                } else if (key_dist == "uniform") {
                    query_key = new UniformGenerator(0, initial_elements - 1);
                } else if (key_dist == "latest") {
                    query_key = new SkewedLatestGenerator(insert_key, zipfian_const);
                } else if (key_dist == "counter") {
                    insert_key.set(0);
                    query_key = new CounterGenerator(0);
                } else {
                    assert(0);
                }
            }

            ~WorkloadBuilder() {
                if (query_key) {
                    delete query_key;
                    query_key = nullptr;
                }
            }

            void fill_record(OpRecord &record) {
                record.type = op_type.next();
                if (record.type == INSERT) {
                    record.key = insert_key.next();
                } else {
                    record.key = query_key->next();
                }
                if (record.type == SCAN) {
                    record.scan_len = scan_len.next();
                }
            }

            static WorkloadBuilder *Create(const std::string &name, size_t initial_elements, double zipfian_const) {
                int percentage[5] = {0};
                std::string key_dist;
                if (name == "ycsb-a") {
                    percentage[READ] = 50;
                    percentage[UPDATE] = 50;
                    key_dist = "zipfian";
                } else if (name == "ycsb-b") {
                    percentage[READ] = 95;
                    percentage[UPDATE] = 5;
                    key_dist = "zipfian";
                } else if (name == "ycsb-c") {
                    percentage[READ] = 100;
                    key_dist = "zipfian";
                } else if (name == "ycsb-d") {
                    percentage[READ] = 95;
                    percentage[INSERT] = 5;
                    key_dist = "latest";
                } else if (name == "ycsb-e") {
                    percentage[SCAN] = 95;
                    percentage[INSERT] = 5;
                    key_dist = "zipfian";
                } else if (name == "ycsb-f") {
                    percentage[READ] = 50;
                    percentage[READMODIFYWRITE] = 50;
                    key_dist = "zipfian";
                } else if (name == "update-only") {
                    percentage[UPDATE] = 100;
                    key_dist = "zipfian";
                } else if (name == "read-only") {
                    percentage[READ] = 100;
                    key_dist = "zipfian";
                } else {
                    fprintf(stderr, "unknown workload %s\n", name.c_str());
                    return nullptr;
                }
                return new WorkloadBuilder(initial_elements, percentage, key_dist, zipfian_const);
            }

        private:
            DiscreteGenerator<OpType> op_type;
            CounterGenerator insert_key;
            UniformGenerator scan_len;
            Generator *query_key;
        };
    }
}

#endif //SDS_YCSB_H

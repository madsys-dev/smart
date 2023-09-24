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

#include "util/ycsb.h"

#include <thread>

using namespace sds::util;

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <type> <initial_elements> <record_count> <dataset-path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int initial_elements = atoi(argv[2]);
    int record_count = atoi(argv[3]);

    WorkloadBuilder *builder = WorkloadBuilder::Create(argv[1], initial_elements, 0.99);
    OpRecord *records = new OpRecord[record_count];
    if (!builder) {
        exit(EXIT_FAILURE);
    }

    FILE *fout = fopen(argv[4], "wb");
    if (!fout) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    const static int kThreads = 8;
    std::thread workers[kThreads];
    for (int i = 0; i < kThreads; ++i) {
        workers[i] = std::thread([&](int tid) {
            int start_off = record_count / kThreads * tid;
            int stop_off = std::min(start_off + record_count / kThreads, record_count);
            for (int off = start_off; off < stop_off; ++off) {
                builder->fill_record(records[off]);
            }
        }, i);
    }

    for (int i = 0; i < kThreads; ++i) {
        workers[i].join();
    }

    if (fwrite(records, sizeof(OpRecord), record_count, fout) != record_count) {
        perror("fwrite");
    }

    fclose(fout);
    delete builder;
    return 0;
}

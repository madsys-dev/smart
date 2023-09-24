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

#include "util/json_config.h"
#include "smart/benchmark.h"
#include "hashtable.h"

int main(int argc, char **argv) {
    using namespace sds;
    using namespace sds::datastructure;
    const char *path = ROOT_DIR "/config/datastructure.json";
    if (getenv("APP_CONFIG_PATH")) {
        path = getenv("APP_CONFIG_PATH");
    }
    JsonConfig config = JsonConfig::load_file(path);
    if (config.get("memory_servers").size() > 1) {
        BenchmarkRunner<RemoteHashTableMultiShard> runner(config, argc, argv);
        if (runner.spawn()) {
            exit(EXIT_FAILURE);
        }
    } else {
        BenchmarkRunner<RemoteHashTable> runner(config, argc, argv);
        if (runner.spawn()) {
            exit(EXIT_FAILURE);
        }
    }
    return 0;
}
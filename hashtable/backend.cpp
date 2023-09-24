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
#include "smart/target.h"

#include "hashtable.h"

using namespace sds;

int main(int argc, char **argv) {
    WritePidFile();
    const char *path = ROOT_DIR "/config/backend.json";
    if (argc == 2) {
        path = argv[1];
    }
    JsonConfig config = JsonConfig::load_file(path);
    BindCore((int) config.get("nic_numa_node").get_int64());
    std::string dev_dax_path = ""; // config.get("dev_dax_path").get_str();
    size_t capacity = config.get("capacity").get_uint64() * kMegaBytes;
    uint16_t tcp_port = (uint16_t) config.get("tcp_port").get_int64();
    Target target;
    void *mmap_addr = mapping_memory(dev_dax_path, capacity);
    int rc = target.register_main_memory(mmap_addr, capacity);
    assert(!rc);
    RemoteHashTable::Setup(target);
    SDS_INFO("Press C to stop the memory node daemon.");
    target.start(tcp_port);
    while (getchar() != 'c') { sleep(1); }
    target.stop();
    return 0;
}

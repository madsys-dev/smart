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

#ifndef SDS_CONFIG_H
#define SDS_CONFIG_H

#include <cstdint>

#include "common.h"
#include "util/json_config.h"

namespace sds {
    struct SmartConfig {
        std::string infiniband_name;
        uint8_t infiniband_port;
        int max_cqe_size;
        int total_uuars;
        int private_uuars;
        int shared_uuars;
        int max_nodes;
        uint32_t max_wqe_size;
        uint32_t max_sge_size;
        uint32_t max_inline_data;
        uint8_t gid_idx;
        bool uuar_affinity_enabled;
        bool shared_cq;
        bool throttler;
        int initial_credit;
        int max_credit;
        int credit_step;
        int execution_epochs;
        uint64_t sample_cycles;
        double inf_credit_weight;
        bool throttler_auto_tuning;
        uint64_t initiator_cache_size;
        bool qp_sharing;
        bool use_conflict_avoidance;
        bool use_speculative_lookup;
        
    public:
        SmartConfig() {
            throttler = true;
            const char *path = getenv("SMART_CONFIG_PATH");
            JsonConfig config = JsonConfig::load_file(path ? path : ROOT_DIR "/config/smart_config.json");
            char buf[11];
            infiniband_name = config.get("infiniband").get("name").get_str();
            infiniband_port = (uint8_t) config.get("infiniband").get("port").get_int64();
            max_cqe_size = (int) config.get("qp_param").get("max_cqe_size").get_int64();
            max_wqe_size = (uint32_t) config.get("qp_param").get("max_wqe_size").get_uint64();
            max_sge_size = (uint32_t) config.get("qp_param").get("max_sge_size").get_uint64();
            max_inline_data = (uint32_t) config.get("qp_param").get("max_inline_data").get_uint64();

            JsonConfig entry = config.get("infiniband").get("gid_idx");
            gid_idx = (uint8_t) (entry.exists() ? entry.get_uint64() : 1);

            max_nodes = (int) config.get("max_nodes").get_int64();
            initiator_cache_size = config.get("initiator_cache_size").get_uint64() * 1024 * 1024;

            bool has_preload = getenv("LD_PRELOAD");
            if (!has_preload) {
                SDS_WARN("*** LD_PRELOAD is not used ***");
                SDS_INFO("reset option 'use_thread_aware_alloc' to false");
            }

            use_conflict_avoidance = config.get("use_conflict_avoidance").get_bool();
            use_speculative_lookup = config.get("use_speculative_lookup").get_bool();
            if (getenv("SMART_OPTS")) {
                std::string str = getenv("SMART_OPTS");
                use_conflict_avoidance = (str.find("ConflictAvoid") != str.npos);
                use_speculative_lookup = (str.find("SpecLookup") != str.npos);
            }
            if (use_conflict_avoidance) {
                unsetenv("DISABLE_CONFLICT_AVOIDANCE");
            } else {
                setenv("DISABLE_CONFLICT_AVOIDANCE", "1", 0);
            }

            bool enable = config.get("use_thread_aware_alloc").get_bool();
            if (getenv("SMART_OPTS")) {
                std::string str = getenv("SMART_OPTS");
                enable = (str.find("ThrdAwareAlloc") != str.npos);
            }

            if (has_preload && enable) {
                total_uuars = (int) config.get("thread_aware_alloc").get("total_uuar").get_uint64();
                shared_uuars = (int) config.get("thread_aware_alloc").get("shared_uuar").get_uint64();
                assert(total_uuars > shared_uuars);
                private_uuars = total_uuars - shared_uuars;
                uuar_affinity_enabled = true;
                shared_cq = config.get("thread_aware_alloc").get("shared_cq").get_bool();

                snprintf(buf, 11, "%d", total_uuars);
                setenv("MLX5_TOTAL_UUARS", buf, 1);
                snprintf(buf, 11, "%d", private_uuars);
                setenv("MLX5_NUM_LOW_LAT_UUARS", buf, 1);
            } else {
                total_uuars = 16;
                shared_uuars = 12;
                private_uuars = total_uuars - shared_uuars;
                uuar_affinity_enabled = false;
                shared_cq = false;
            }

            enable = config.get("use_work_req_throt").get_bool();
            if (getenv("SMART_OPTS")) {
                std::string str = getenv("SMART_OPTS");
                enable = (str.find("WorkReqThrot") != str.npos);
            }

            if (enable) {
                throttler_auto_tuning = config.get("work_req_throt").get("auto_tuning").get_bool();
                initial_credit = (int) config.get("work_req_throt").get("initial_credit").get_uint64();
            } else {
                throttler_auto_tuning = false;
                initial_credit = 4096;
            }
            
            max_credit = (int) config.get("work_req_throt").get("max_credit").get_uint64();
            credit_step = (int) config.get("work_req_throt").get("credit_step").get_uint64();
            execution_epochs = (int) config.get("work_req_throt").get("execution_epochs").get_uint64();
            sample_cycles = config.get("work_req_throt").get("sample_cycles").get_uint64();
            inf_credit_weight = config.get("work_req_throt").get("inf_credit_weight").get_double();
            qp_sharing = config.get("experimental").get("qp_sharing").get_bool();
        }

        SmartConfig(const SmartConfig &) = delete;

        SmartConfig &operator=(const SmartConfig &) = delete;
    };
}

#endif //SDS_CONFIG_H

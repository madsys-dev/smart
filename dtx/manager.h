// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#ifndef SDS_DTX_MANAGER_H
#define SDS_DTX_MANAGER_H

#include "util/json_config.h"
#include "smart/initiator.h"

#include "addr_cache.h"
#include "smart/backoff.h"

using namespace sds;

extern thread_local uint64_t rdma_cnt;

class DTXContext {
public:
    DTXContext(JsonConfig &config, int max_threads) : config_(config) {
        for (int i = 0; i < kMaxThreads; ++i) {
            tl_data_[i].log_alloc = new LogOffsetAllocator(i, kMaxThreads);
            const static size_t kBufferSize = 8 * 1024 * 1024;
            char *buf = (char *) node_.alloc_cache(kBufferSize);
            assert(buf);
            tl_data_[i].buf_alloc = new RDMABufferAllocator(buf, buf + kBufferSize);
        }

        auto memory_servers = config.get("memory_servers");
        remote_nodes_ = memory_servers.size();
        for (int node_id = 0; node_id < memory_servers.size(); ++node_id) {
            auto entry = memory_servers.get(node_id);
            std::string domain_name = entry.get("hostname").get_str();
            uint16_t tcp_port = (uint16_t) entry.get("port").get_int64();
            if (node_.connect(node_id, domain_name.c_str(), tcp_port, max_threads)) {
                exit(EXIT_FAILURE);
            }
        }

        LoadMetadata();
    }

    DTXContext(const DTXContext &) = delete;

    DTXContext &operator=(const DTXContext &) = delete;

    void Write(const void *local, GlobalAddress remote, size_t length) {
        int rc = node_.write(local, remote, length);
        assert(!rc);
        rdma_cnt++;
    }

    void read(const void *local, GlobalAddress remote, size_t length) {
        int rc = node_.read(local, remote, length);
        assert(!rc);
        rdma_cnt++;
    }

    void CompareAndSwap(const void *local, GlobalAddress remote, uint64_t compare_val, uint64_t swap_val) {
        int rc = node_.compare_and_swap(local, remote, compare_val, swap_val);
        assert(!rc);
        rdma_cnt++;
    }

    void FetchAndAdd(const void *local, GlobalAddress remote, uint64_t add_val) {
        int rc = node_.fetch_and_add(local, remote, add_val);
        assert(!rc);
        rdma_cnt++;
    }

    void PostRequest() {
        int rc = node_.post_request();
        assert(!rc);
    }

    void Sync() {
        int rc = node_.sync();
        assert(!rc);
    }

    char *Alloc(size_t size) { return tl_data_[GetThreadID()].buf_alloc->Alloc(size); }

    int GetRemoteNodes() const { return remote_nodes_; }

    std::function<void()> GetPollTask(int &running_tasks) {
        return node_.get_poll_task(running_tasks);
    }

    void RunTasks() {
        return node_.run_tasks();
    }

    AddrCache *GetAddrCache() {
        return &tl_data_[GetThreadID()].addr_cache;
    }

    node_id_t GetPrimaryNodeID(table_id_t table_id) {
        auto search = primary_table_nodes.find(table_id);
        assert(search != primary_table_nodes.end());
        return search->second;
    }

    std::vector<node_id_t> *GetBackupNodeID(table_id_t table_id) {
        if (backup_table_nodes.empty()) {
            return nullptr;
        }
        auto search = backup_table_nodes.find(table_id);
        assert(search != backup_table_nodes.end());
        return &(search->second);
    }

    HashMeta &GetPrimaryHashMetaWithTableID(table_id_t table_id) {
        auto search = primary_hash_metas.find(table_id);
        assert(search != primary_hash_metas.end());
        return search->second;
    }

    std::vector<HashMeta> *GetBackupHashMetasWithTableID(table_id_t table_id) {
        if (backup_hash_metas.empty()) {
            return nullptr;
        }
        auto search = backup_hash_metas.find(table_id);
        assert(search != backup_hash_metas.end());
        return &(search->second);
    }

    offset_t GetNextLogOffset(node_id_t node_id, size_t log_size) {
        return log_base_[node_id] + tl_data_[GetThreadID()].log_alloc->GetNextLogOffset(node_id, log_size);
    }

    void LoadMetadata() {
        for (int node_id = 0; node_id < remote_nodes_; ++node_id) {
            uint64_t offset;
            int rc = node_.get_root_entry(node_id, 255, offset);
            assert(!rc);
            log_base_[node_id] = offset;
            rc = node_.get_root_entry(node_id, 0, offset);
            assert(!rc);
            table_id_t nr_tables = offset;
            HashMeta *hash_meta = (HashMeta *) node_.alloc_cache(sizeof(HashMeta));
            assert(hash_meta);
            for (table_id_t table_id = 1; table_id <= nr_tables; ++table_id) {
                rc = node_.get_root_entry(node_id, table_id, offset);
                assert(!rc);
                rc = node_.read(hash_meta, GlobalAddress(node_id, offset),
                                sizeof(HashMeta), Initiator::Option::Sync);
                assert(!rc);
                // SDS_INFO("%ld: %lx %ld %ld", hash_meta->table_id, hash_meta->base_off,
                //          hash_meta->bucket_num, hash_meta->node_size);
                if (node_id == table_id % remote_nodes_) {
                    primary_hash_metas[table_id] = *hash_meta;
                    primary_table_nodes[table_id] = node_id;
                } else if (backup_hash_metas[table_id].size() < BACKUP_DEGREE) {
                    backup_hash_metas[table_id].push_back(*hash_meta);
                    backup_table_nodes[table_id].push_back(node_id);
                }
            }
        }
    }

    void BeginTask() {
        tl_data_[GetThreadID()].backoff.begin_task();
    }

    void EndTask() {
        tl_data_[GetThreadID()].backoff.end_task();
    }

    void RetryTask() {
        tl_data_[GetThreadID()].backoff.retry_task();
    }

private:
    struct ThreadLocal {
        AddrCache addr_cache;
        LogOffsetAllocator *log_alloc;
        RDMABufferAllocator *buf_alloc;
        Backoff backoff;
        uint64_t padding[16];
        // ThreadLocal(): task_throttler(0.15, 0.25) {}
    };

    std::unordered_map<table_id_t, HashMeta> primary_hash_metas;
    std::unordered_map<table_id_t, std::vector<HashMeta>> backup_hash_metas;
    std::unordered_map<table_id_t, node_id_t> primary_table_nodes;
    std::unordered_map<table_id_t, std::vector<node_id_t>> backup_table_nodes;

    JsonConfig config_;
    Initiator node_;
    offset_t log_base_[NUM_MEMORY_NODES];
    int remote_nodes_;
    alignas(128) ThreadLocal tl_data_[kMaxThreads];
};

#endif // SDS_DTX_MANAGER_H
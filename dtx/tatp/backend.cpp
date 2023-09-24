// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#include "util/json_config.h"
#include "smart/target.h"
#include "smart/thread.h"

#include "../common.h"
#include "../memstore.h"
#include "tatp.h"

using namespace sds;

void setup(Target &target) {
    static_assert(MAX_ITEM_SIZE == 40, "");
    uint64_t hash_buf_size = 4ull * 1024 * 1024 * 1024;

    char *hash_buffer = (char *) target.alloc_chunk(hash_buf_size / kChunkSize);
    offset_t reserve_start = hash_buf_size * 0.75;
    char *hash_reserve_buffer = hash_buffer + reserve_start;

    char *log_buffer = (char *) target.alloc_chunk(LOG_BUFFER_SIZE / kChunkSize);
    target.set_root_entry(255, target.rel_ptr(log_buffer).raw);

    memset(hash_buffer, 0, hash_buf_size);
    memset(log_buffer, 0, LOG_BUFFER_SIZE);

    MemStoreAllocParam mem_store_alloc_param((char *) target.base_address(), hash_buffer, 0, hash_reserve_buffer);
    MemStoreReserveParam mem_store_reserve_param(hash_reserve_buffer, 0, hash_buffer + hash_buf_size);
    std::vector<HashStore *> all_tables;
    auto tatp = new TATP();
    tatp->LoadTable(&mem_store_alloc_param, &mem_store_reserve_param);
    all_tables = tatp->GetHashStore();
    auto *hash_meta = (HashMeta *) target.alloc_chunk((all_tables.size() * sizeof(HashMeta)) / kChunkSize + 1);
    int i = 0;
    for (auto &hash_table: all_tables) {
        new(&hash_meta[i]) HashMeta(hash_table->GetTableID(), (uint64_t) hash_table->GetDataPtr(),
                                    hash_table->GetBucketNum(), hash_table->GetHashNodeSize(),
                                    hash_table->GetBaseOff());
        SDS_INFO("%ld: %lx %ld %ld", hash_meta[i].table_id, hash_meta[i].base_off, hash_meta[i].bucket_num,
                 hash_meta[i].node_size);
        target.set_root_entry(hash_table->GetTableID(), target.rel_ptr(&hash_meta[i]).raw);
        ++i;
    }

    target.set_root_entry(0, i);
}

int main(int argc, char **argv) {
    WritePidFile();
    const char *path = ROOT_DIR "/config/backend.json";
    if (argc == 2) {
        path = argv[1];
    }
    JsonConfig config = JsonConfig::load_file(path);
    BindCore((int) config.get("nic_numa_node").get_int64());
    std::string dev_dax_path = config.get("dev_dax_path").get_str();
    size_t capacity = config.get("capacity").get_uint64() * kMegaBytes;
    uint16_t tcp_port = (uint16_t) config.get("tcp_port").get_int64();
    Target target;
    void *mmap_addr = mapping_memory(dev_dax_path, capacity);
    int rc = target.register_main_memory(mmap_addr, capacity);
    assert(!rc);
    setup(target);
    SDS_INFO("Press C to stop the memory node daemon.");
    target.start(tcp_port);
    while (getchar() != 'c') { sleep(1); }
    target.stop();
    return 0;
}

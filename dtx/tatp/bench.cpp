// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>

#include "../dtx.h"
#include "tatp.h"

using namespace std::placeholders;

size_t kMaxTransactions = 1000000;
pthread_barrier_t barrier;
uint64_t threads;
uint64_t coroutines;

std::atomic<uint64_t> attempts(0);
std::atomic<uint64_t> commits(0);
double *timer;
std::atomic<uint64_t> tx_id_generator(0);

thread_local size_t ATTEMPTED_NUM;
thread_local uint64_t seed;
thread_local TATP *tatp_client;
thread_local TATPTxType *workgen_arr;

thread_local uint64_t rdma_cnt;
std::atomic<uint64_t> rdma_cnt_sum(0);

bool TxGetSubsciberData(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);
    // Build key for the database record
    tatp_sub_key_t sub_key;
    sub_key.s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    // This empty data sub_obj will be filled by RDMA reading from remote when running transaction
    auto sub_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSubscriberTable, sub_key.item_key);
    // Add r/w set and execute transaction
    dtx->AddToReadOnlySet(sub_obj);
    if (!dtx->TxExe()) return false;
    // Get value
    auto *value = (tatp_sub_val_t *) (sub_obj->value);
    assert(value->msc_location == tatp_sub_msc_location_magic);
    // Commit transaction
    bool commit_status = dtx->TxCommit();
    return commit_status;
}

bool TxGetNewDestination(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);
    uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    uint8_t sf_type = (FastRand(&seed) % 4) + 1;
    uint8_t start_time = (FastRand(&seed) % 3) * 8;
    uint8_t end_time = (FastRand(&seed) % 24) * 1;
    unsigned cf_to_fetch = (start_time / 8) + 1;
    assert(cf_to_fetch >= 1 && cf_to_fetch <= 3);
    /* Fetch a single special facility record */
    tatp_specfac_key_t specfac_key;
    specfac_key.s_id = s_id;
    specfac_key.sf_type = sf_type;
    auto specfac_obj =
            std::make_shared<DataItem>((table_id_t) TATPTableType::kSpecialFacilityTable, specfac_key.item_key);
    dtx->AddToReadOnlySet(specfac_obj);
    if (!dtx->TxExe()) return false;
    if (specfac_obj->value_size == 0) {
        dtx->TxAbortReadOnly();
        return false;
    }

    // Need to wait for reading specfac_obj from remote
    auto *specfac_val = (tatp_specfac_val_t *) (specfac_obj->value);
    assert(specfac_val->data_b[0] == tatp_specfac_data_b0_magic);
    if (specfac_val->is_active == 0) {
        // is_active is randomly generated at pm node side
        dtx->TxAbortReadOnly();
        return false;
    }

    /* Fetch possibly multiple call forwarding records. */
    DataItemPtr callfwd_obj[3];
    tatp_callfwd_key_t callfwd_key[3];

    for (unsigned i = 0; i < cf_to_fetch; i++) {
        callfwd_key[i].s_id = s_id;
        callfwd_key[i].sf_type = sf_type;
        callfwd_key[i].start_time = (i * 8);
        callfwd_obj[i] = std::make_shared<DataItem>(
                (table_id_t) TATPTableType::kCallForwardingTable,
                callfwd_key[i].item_key);
        dtx->AddToReadOnlySet(callfwd_obj[i]);
    }
    if (!dtx->TxExe()) return false;

    bool callfwd_success = false;
    for (unsigned i = 0; i < cf_to_fetch; i++) {
        if (callfwd_obj[i]->value_size == 0) {
            continue;
        }

        auto *callfwd_val = (tatp_callfwd_val_t *) (callfwd_obj[i]->value);
        assert(callfwd_val->numberx[0] == tatp_callfwd_numberx0_magic);
        if (callfwd_key[i].start_time <= start_time && end_time < callfwd_val->end_time) {
            /* All conditions satisfied */
            callfwd_success = true;
        }
    }

    if (callfwd_success) {
        bool commit_status = dtx->TxCommit();
        return commit_status;
    } else {
        dtx->TxAbortReadOnly();
        return false;
    }
}

bool TxGetAccessData(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);
    tatp_accinf_key_t key;
    key.s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    key.ai_type = (FastRand(&seed) & 3) + 1;
    auto acc_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kAccessInfoTable, key.item_key);
    dtx->AddToReadOnlySet(acc_obj);
    if (!dtx->TxExe()) return false;
    if (acc_obj->value_size > 0) {
        /* The key was found */
        auto *value = (tatp_accinf_val_t *) (acc_obj->value);
        assert(value->data1 == tatp_accinf_data1_magic);
        bool commit_status = dtx->TxCommit();
        return commit_status;
    } else {
        /* Key not found */
        dtx->TxAbortReadOnly();
        return false;
    }
}

bool TxUpdateSubscriberData(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);
    uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    uint8_t sf_type = (FastRand(&seed) % 4) + 1;
    /* Read + lock the subscriber record */
    tatp_sub_key_t sub_key;
    sub_key.s_id = s_id;
    auto sub_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSubscriberTable, sub_key.item_key);
    dtx->AddToReadWriteSet(sub_obj);
    /* Read + lock the special facilty record */
    tatp_specfac_key_t specfac_key;
    specfac_key.s_id = s_id;
    specfac_key.sf_type = sf_type;

    auto specfac_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSpecialFacilityTable,
                                                  specfac_key.item_key);
    dtx->AddToReadWriteSet(specfac_obj);
    if (!dtx->TxExe()) return false;

    /* If we are here, execution succeeded and we have locks */
    auto *sub_val = (tatp_sub_val_t *) (sub_obj->value);
    assert(sub_val->msc_location == tatp_sub_msc_location_magic);
    sub_val->bits = FastRand(&seed); /* Update */
    auto *specfac_val = (tatp_specfac_val_t *) (specfac_obj->value);
    assert(specfac_val->data_b[0] == tatp_specfac_data_b0_magic);
    specfac_val->data_a = FastRand(&seed); /* Update */
    bool commit_status = dtx->TxCommit();
    return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Update a SUBSCRIBER row
bool TxUpdateLocation(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    uint32_t vlr_location = FastRand(&seed);

    /* Read the secondary subscriber record */
    tatp_sec_sub_key_t sec_sub_key;
    sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

    auto sec_sub_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSecSubscriberTable,
                                                  sec_sub_key.item_key);

    dtx->AddToReadOnlySet(sec_sub_obj);
    if (!dtx->TxExe()) return false;

    auto *sec_sub_val = (tatp_sec_sub_val_t *) (sec_sub_obj->value);
    assert(sec_sub_val->magic == tatp_sec_sub_magic);
    assert(sec_sub_val->s_id == s_id);
    tatp_sub_key_t sub_key;
    sub_key.s_id = sec_sub_val->s_id;

    auto sub_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSubscriberTable, sub_key.item_key);

    dtx->AddToReadWriteSet(sub_obj);
    if (!dtx->TxExe()) return false;

    auto *sub_val = (tatp_sub_val_t *) (sub_obj->value);
    assert(sub_val->msc_location == tatp_sub_msc_location_magic);
    sub_val->vlr_location = vlr_location; /* Update */

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

bool TxInsertCallForwarding(tx_id_t tx_id, DTX *dtx) {
    // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxInsertCallForwarding, tx_id=" << tx_id;
    dtx->TxBegin(tx_id);

    uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    uint8_t sf_type = (FastRand(&seed) % 4) + 1;
    uint8_t start_time = (FastRand(&seed) % 3) * 8;
    uint8_t end_time = (FastRand(&seed) % 24) * 1;

    // Read the secondary subscriber record
    tatp_sec_sub_key_t sec_sub_key;
    sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);

    auto sec_sub_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSecSubscriberTable,
                                                  sec_sub_key.item_key);

    dtx->AddToReadOnlySet(sec_sub_obj);
    if (!dtx->TxExe()) return false;

    auto *sec_sub_val = (tatp_sec_sub_val_t *) (sec_sub_obj->value);
    assert(sec_sub_val->magic == tatp_sec_sub_magic);
    assert(sec_sub_val->s_id == s_id);

    // Read the Special Facility record
    tatp_specfac_key_t specfac_key;
    specfac_key.s_id = s_id;
    specfac_key.sf_type = sf_type;

    auto specfac_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSpecialFacilityTable,
                                                  specfac_key.item_key);

    dtx->AddToReadOnlySet(specfac_obj);
    if (!dtx->TxExe()) return false;

    /* The Special Facility record exists only 62.5% of the time */
    if (specfac_obj->value_size == 0) {
        dtx->TxAbortReadOnly();
        return false;
    }

    /* If we are here, the Special Facility record exists. */
    auto *specfac_val = (tatp_specfac_val_t *) (specfac_obj->value);
    assert(specfac_val->data_b[0] == tatp_specfac_data_b0_magic);

    // Lock the Call Forwarding record
    tatp_callfwd_key_t callfwd_key;
    callfwd_key.s_id = s_id;
    callfwd_key.sf_type = sf_type;
    callfwd_key.start_time = start_time;

    auto callfwd_obj = std::make_shared<DataItem>(
            (table_id_t) TATPTableType::kCallForwardingTable,
            sizeof(tatp_callfwd_val_t),
            callfwd_key.item_key,
            tx_id,  // Using tx_id as key's version
            1);

    // Handle Insert. Only read the remote offset of callfwd_obj
    dtx->AddToReadWriteSet(callfwd_obj);
    if (!dtx->TxExe()) return false;

    // Fill callfwd_val by user
    auto *callfwd_val = (tatp_callfwd_val_t *) (callfwd_obj->value);
    callfwd_val->numberx[0] = tatp_callfwd_numberx0_magic;
    callfwd_val->end_time = end_time;

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

// 1. Read a SECONDARY_SUBSCRIBER row
// 2. Delete a CALL_FORWARDING row
bool TxDeleteCallForwarding(tx_id_t tx_id, DTX *dtx) {
    // RDMA_LOG(DBG) << "coro " << dtx->coro_id << " executes TxDeleteCallForwarding, tx_id=" << tx_id;
    dtx->TxBegin(tx_id);

    uint32_t s_id = tatp_client->GetNonUniformRandomSubscriber(&seed);
    uint8_t sf_type = (FastRand(&seed) % 4) + 1;
    uint8_t start_time = (FastRand(&seed) % 3) * 8;

    // Read the secondary subscriber record
    tatp_sec_sub_key_t sec_sub_key;
    sec_sub_key.sub_number = tatp_client->FastGetSubscribeNumFromSubscribeID(s_id);
    auto
            sec_sub_obj = std::make_shared<DataItem>((table_id_t) TATPTableType::kSecSubscriberTable,
                                                     sec_sub_key.item_key);
    dtx->AddToReadOnlySet(sec_sub_obj);
    if (!dtx->TxExe()) return false;

    auto *sec_sub_val = (tatp_sec_sub_val_t *) (sec_sub_obj->value);
    assert(sec_sub_val->magic == tatp_sec_sub_magic);
    assert(sec_sub_val->s_id == s_id);

    // Delete the Call Forwarding record if it exists
    tatp_callfwd_key_t callfwd_key;
    callfwd_key.s_id = s_id;
    callfwd_key.sf_type = sf_type;
    callfwd_key.start_time = start_time;

    auto callfwd_obj = std::make_shared<DataItem>(
            (table_id_t) TATPTableType::kCallForwardingTable,
            callfwd_key.item_key);

    dtx->AddToReadWriteSet(callfwd_obj);
    if (!dtx->TxExe()) return false;
    auto *callfwd_val = (tatp_callfwd_val_t *) (callfwd_obj->value);
    assert(callfwd_val->numberx[0] == tatp_callfwd_numberx0_magic);
    callfwd_obj->valid = 0;
    bool commit_status = dtx->TxCommit();
    return commit_status;
}

thread_local int running_tasks;


void WarmUp(DTXContext *context) {
    DTX *dtx = new DTX(context);
    bool tx_committed = false;
    for (int i = 0; i < 50000; ++i) {
        TATPTxType tx_type = workgen_arr[FastRand(&seed) % 100];
        uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
        switch (tx_type) {
            case TATPTxType::kGetSubsciberData:
                tx_committed = TxGetSubsciberData(iter, dtx);
                break;
            case TATPTxType::kGetNewDestination:
                tx_committed = TxGetNewDestination(iter, dtx);
                break;
            case TATPTxType::kGetAccessData:
                tx_committed = TxGetAccessData(iter, dtx);
                break;
            case TATPTxType::kUpdateSubscriberData:
                tx_committed = TxUpdateSubscriberData(iter, dtx);
                break;
            case TATPTxType::kUpdateLocation:
                tx_committed = TxUpdateLocation(iter, dtx);
                break;
            case TATPTxType::kInsertCallForwarding:
                tx_committed = TxInsertCallForwarding(iter, dtx);
                break;
            case TATPTxType::kDeleteCallForwarding:
                tx_committed = TxDeleteCallForwarding(iter, dtx);
                break;
            default:
                printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
                abort();
        }
    }
    delete dtx;
}

const static uint64_t kCpuFrequency = 2400;
uint64_t g_idle_cycles = 0;

static void IdleExecution() {
    if (g_idle_cycles) {
        uint64_t start_tsc = rdtsc();
        while (rdtsc() - start_tsc < g_idle_cycles) {
            YieldTask();
        }
    }
}

void RunTx(DTXContext *context) {
    DTX *dtx = new DTX(context);
    struct timespec tx_start_time, tx_end_time;
    bool tx_committed = false;
    uint64_t attempt_tx = 0;
    uint64_t commit_tx = 0;
    int timer_idx = GetThreadID() * coroutines + GetTaskID();
    // Running transactions
    while (true) {
        TATPTxType tx_type = workgen_arr[FastRand(&seed) % 100];
        uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
        attempt_tx++;
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
#ifdef ABORT_DISCARD
        switch (tx_type) {
            case TATPTxType::kGetSubsciberData:
                do {
                    tx_committed = TxGetSubsciberData(iter, dtx);
                } while (!tx_committed);
                break;
            case TATPTxType::kGetNewDestination:
                do {
                    tx_committed = TxGetNewDestination(iter, dtx);
                } while (!tx_committed);
                break;
            case TATPTxType::kGetAccessData:
                do {
                    tx_committed = TxGetAccessData(iter, dtx);
                } while (!tx_committed);
                break;
            case TATPTxType::kUpdateSubscriberData:
                do {
                    tx_committed = TxUpdateSubscriberData(iter, dtx);
                } while (!tx_committed);
                break;
            case TATPTxType::kUpdateLocation:
                do {
                    tx_committed = TxUpdateLocation(iter, dtx);
                } while (!tx_committed);
                break;
            case TATPTxType::kInsertCallForwarding:
                do {
                    tx_committed = TxInsertCallForwarding(iter, dtx);
                } while (!tx_committed);
                break;
            case TATPTxType::kDeleteCallForwarding:
                do {
                    tx_committed = TxDeleteCallForwarding(iter, dtx);
                } while (!tx_committed);
                break;
            default:
                printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
                abort();
        }
#else
        switch (tx_type) {
            case TATPTxType::kGetSubsciberData:
                tx_committed = TxGetSubsciberData(iter, dtx);
                break;
            case TATPTxType::kGetNewDestination:
                tx_committed = TxGetNewDestination(iter, dtx);
                break;
            case TATPTxType::kGetAccessData:
                tx_committed = TxGetAccessData(iter, dtx);
                break;
            case TATPTxType::kUpdateSubscriberData:
                tx_committed = TxUpdateSubscriberData(iter, dtx);
                break;
            case TATPTxType::kUpdateLocation:
                tx_committed = TxUpdateLocation(iter, dtx);
                break;
            case TATPTxType::kInsertCallForwarding:
                tx_committed = TxInsertCallForwarding(iter, dtx);
                break;
            case TATPTxType::kDeleteCallForwarding:
                tx_committed = TxDeleteCallForwarding(iter, dtx);
                break;
            default:
                printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
                abort();
        }
#endif
        // Stat after one transaction finishes
        if (tx_committed) {
            clock_gettime(CLOCK_REALTIME, &tx_end_time);
            double tx_usec = (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
                             (double) (tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
            timer[timer_idx] = tx_usec;
            timer_idx += threads * coroutines;
            commit_tx++;
            IdleExecution();
        }
        // Stat after a million of transactions finish
        if (attempt_tx == ATTEMPTED_NUM) {
            attempts.fetch_add(attempt_tx);
            commits.fetch_add(commit_tx);
            break;
        }
    }
    running_tasks--;
    delete dtx;
}

void execute_thread(int id, DTXContext *context) {
    BindCore(id);
#if 0
    assert(coroutines == 1);
    ATTEMPTED_NUM = kMaxTransactions / threads;
    seed = rdtsc() * kMaxThreads + id;
    tatp_client = new TATP();
    workgen_arr = tatp_client->CreateWorkgenArray();
    pthread_barrier_wait(&barrier);
    RunTx(manager);
    pthread_barrier_wait(&barrier);
    rdma_cnt_sum += rdma_cnt;
#endif
    ATTEMPTED_NUM = kMaxTransactions / threads / coroutines;
    auto hostname = GetHostName();
    seed = MurmurHash3_x86_32(hostname.c_str(), hostname.length(), 0xcc9e2d51) * kMaxThreads + id;
    tatp_client = new TATP();
    workgen_arr = tatp_client->CreateWorkgenArray();
    WarmUp(context);
    TaskPool::Enable();
    auto &task_pool = TaskPool::Get();
    running_tasks = coroutines;

    pthread_barrier_wait(&barrier);
    task_pool.spawn(context->GetPollTask(running_tasks));
    for (int i = 0; i < coroutines; ++i) {
        task_pool.spawn(std::bind(&RunTx, context), 128 * 1024);
    }
    while (!task_pool.empty()) {
        YieldTask();
    }
    pthread_barrier_wait(&barrier);
    rdma_cnt_sum += rdma_cnt;
}

void synchronize_begin(DTXContext *ctx) {
    if (getenv("COMPUTE_NODES")) {
        int nr_compute_nodes = (int) atoi(getenv("COMPUTE_NODES"));
        if (nr_compute_nodes <= 1) return;
        GlobalAddress addr(0, sizeof(SuperChunk));
        uint64_t *buf = (uint64_t *) ctx->Alloc(8);
        ctx->FetchAndAdd(buf, addr, 1);
        ctx->PostRequest();
        ctx->Sync();
        int retry = 0;
        while (true) {
            ctx->read(buf, addr, sizeof(uint64_t));
            ctx->PostRequest();
            ctx->Sync();
            if (*buf == nr_compute_nodes) {
                SDS_INFO("ticket = %ld", *buf);
                break;
            }
            sleep(1);
            retry++;
            SDS_INFO("FAILED ticket = %ld", *buf);
            assert(retry < 60);
        }
    }
}

void synchronize_end(DTXContext *ctx) {
    if (getenv("COMPUTE_NODES")) {
        uint64_t *buf = (uint64_t *) ctx->Alloc(8);
        int nr_compute_nodes = (int) atoi(getenv("COMPUTE_NODES"));
        if (nr_compute_nodes <= 1) return;
        GlobalAddress addr(0, sizeof(SuperChunk));
        ctx->CompareAndSwap(buf, addr, nr_compute_nodes, 0);
        ctx->PostRequest();
        ctx->Sync();
        if (*buf == nr_compute_nodes) {
            SDS_INFO("RESET ticket = 0");
        }
    }
}

void report(double elapsed_time, JsonConfig &config) {
    assert(commits.load() <= kMaxTransactions);
    std::sort(timer, timer + commits.load());
    std::string dump_prefix;
    if (getenv("DUMP_PREFIX")) {
        dump_prefix = std::string(getenv("DUMP_PREFIX"));
    } else {
        dump_prefix = "dtx-tatp";
    }
    SDS_INFO("%s: #thread = %ld, #coro_per_thread = %ld, "
             "attempt txn = %.3lf M/s, committed txn = %.3lf M/s, "
             "P50 latency = %.3lf us, P99 latency = %.3lf us, abort rate = %.3lf, "
             "RDMA ops per txn = %.3lf M, RDMA ops per second = %.3lf M",
             dump_prefix.c_str(),
             threads,
             coroutines,
             attempts.load() / elapsed_time,
             commits.load() / elapsed_time,
             timer[(int) (0.5 * commits.load())],
             timer[(int) (0.99 * commits.load())],
             1.0 - (commits.load() * 1.0 / attempts.load()),
             1.0 * rdma_cnt_sum.load() / attempts.load(),
             rdma_cnt_sum.load() / elapsed_time);
    std::string dump_file_path = config.get("dump_file_path").get_str();
    if (getenv("DUMP_FILE_PATH")) {
        dump_file_path = getenv("DUMP_FILE_PATH");
    }
    if (dump_file_path.empty()) {
        return;
    }
    FILE *fout = fopen(dump_file_path.c_str(), "a+");
    if (!fout) {
        SDS_PERROR("fopen");
        return;
    }
    fprintf(fout, "%s, %ld, %ld, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf\n",
            dump_prefix.c_str(),
            threads,
            coroutines,
            attempts.load() / elapsed_time,
            commits.load() / elapsed_time,
            timer[(int) (0.5 * commits.load())],
            timer[(int) (0.99 * commits.load())],
            1.0 - (commits.load() * 1.0 / attempts.load()),
            1.0 * rdma_cnt_sum.load() / attempts.load(),
            rdma_cnt_sum.load() / elapsed_time);
    fclose(fout);
}

int main(int argc, char **argv) {
    BindCore(1);
    const char *path = ROOT_DIR "/config/transaction.json";
    if (getenv("APP_CONFIG_PATH")) {
        path = getenv("APP_CONFIG_PATH");
    }
    if (getenv("IDLE_USEC")) {
        g_idle_cycles = kCpuFrequency * atoi(getenv("IDLE_USEC"));
    }
    JsonConfig config = JsonConfig::load_file(path);
    kMaxTransactions = config.get("nr_transactions").get_uint64();
    srand48(time(nullptr));
    threads = argc < 2 ? 1 : atoi(argv[1]);
    coroutines = argc < 3 ? 1 : atoi(argv[2]);
    timer = new double[kMaxTransactions];
    DTXContext *context = new DTXContext(config, threads);
    tatp_client = new TATP();
    timespec ts_begin, ts_end;
    pthread_barrier_init(&barrier, nullptr, threads + 1);
    std::vector<std::thread> workers;
    workers.resize(threads);
    synchronize_begin(context);
    for (int i = 0; i < threads; ++i) {
        workers[i] = std::thread(execute_thread, i, context);
    }
    pthread_barrier_wait(&barrier);
    clock_gettime(CLOCK_MONOTONIC, &ts_begin);
    pthread_barrier_wait(&barrier);
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    for (int i = 0; i < threads; ++i) {
        workers[i].join();
    }
    double elapsed_time = (ts_end.tv_sec - ts_begin.tv_sec) * 1000000.0 +
                          (ts_end.tv_nsec - ts_begin.tv_nsec) / 1000.0;
    report(elapsed_time, config);
    synchronize_end(context);
    delete context;
    return 0;
}
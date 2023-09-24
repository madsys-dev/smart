// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>

#include "../dtx.h"
#include "smallbank.h"

using namespace std::placeholders;

size_t kMaxTransactions = 800000;
pthread_barrier_t barrier;
uint64_t threads;
uint64_t coroutines;

std::atomic<uint64_t> attempts(0);
std::atomic<uint64_t> commits(0);
double *timer;
std::atomic<uint64_t> tx_id_generator(0);

thread_local size_t ATTEMPTED_NUM;
thread_local uint64_t seed;
thread_local SmallBank *smallbank_client;
thread_local SmallBankTxType *workgen_arr;
thread_local uint64_t rdma_cnt;
std::atomic<uint64_t> rdma_cnt_sum(0);

bool TxAmalgamate(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    /* Transaction parameters */
    uint64_t acct_id_0, acct_id_1;
    smallbank_client->get_two_accounts(&seed, &acct_id_0, &acct_id_1);

    /* Read from savings and checking tables for acct_id_0 */
    smallbank_savings_key_t sav_key_0;
    sav_key_0.acct_id = acct_id_0;
    auto sav_obj_0 = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kSavingsTable, sav_key_0.item_key);
    dtx->AddToReadWriteSet(sav_obj_0);

    smallbank_checking_key_t chk_key_0;
    chk_key_0.acct_id = acct_id_0;
    auto chk_obj_0 = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key_0.item_key);
    dtx->AddToReadWriteSet(chk_obj_0);

    /* Read from checking account for acct_id_1 */
    smallbank_checking_key_t chk_key_1;
    chk_key_1.acct_id = acct_id_1;
    auto chk_obj_1 = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key_1.item_key);
    dtx->AddToReadWriteSet(chk_obj_1);

    if (!dtx->TxExe()) return false;

    /* If we are here, execution succeeded and we have locks */
    smallbank_savings_val_t *sav_val_0 = (smallbank_savings_val_t *) sav_obj_0->value;
    smallbank_checking_val_t *chk_val_0 = (smallbank_checking_val_t *) chk_obj_0->value;
    smallbank_checking_val_t *chk_val_1 = (smallbank_checking_val_t *) chk_obj_1->value;
    assert(sav_val_0->magic == smallbank_savings_magic);
    assert(chk_val_0->magic == smallbank_checking_magic);
    assert(chk_val_1->magic == smallbank_checking_magic);

    /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
    chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

    sav_val_0->bal = 0;
    chk_val_0->bal = 0;

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

/* Calculate the sum of saving and checking kBalance */
bool TxBalance(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    /* Transaction parameters */
    uint64_t acct_id;
    smallbank_client->get_account(&seed, &acct_id);

    /* Read from savings and checking tables */
    smallbank_savings_key_t sav_key;
    sav_key.acct_id = acct_id;
    auto sav_obj = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kSavingsTable, sav_key.item_key);
    dtx->AddToReadOnlySet(sav_obj);

    smallbank_checking_key_t chk_key;
    chk_key.acct_id = acct_id;
    auto chk_obj = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key.item_key);
    dtx->AddToReadOnlySet(chk_obj);

    if (!dtx->TxExe()) return false;

    smallbank_savings_val_t *sav_val = (smallbank_savings_val_t *) sav_obj->value;
    smallbank_checking_val_t *chk_val = (smallbank_checking_val_t *) chk_obj->value;
    assert(sav_val->magic == smallbank_savings_magic);
    assert(chk_val->magic == smallbank_checking_magic);

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

/* Add $1.3 to acct_id's checking account */
bool TxDepositChecking(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    /* Transaction parameters */
    uint64_t acct_id;
    smallbank_client->get_account(&seed, &acct_id);
    float amount = 1.3;

    /* Read from checking table */
    smallbank_checking_key_t chk_key;
    chk_key.acct_id = acct_id;
    auto chk_obj = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key.item_key);
    dtx->AddToReadWriteSet(chk_obj);

    if (!dtx->TxExe()) return false;

    /* If we are here, execution succeeded and we have a lock*/
    smallbank_checking_val_t *chk_val = (smallbank_checking_val_t *) chk_obj->value;
    assert(chk_val->magic == smallbank_checking_magic);

    chk_val->bal += amount; /* Update checking kBalance */

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool TxSendPayment(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
    uint64_t acct_id_0, acct_id_1;
    smallbank_client->get_two_accounts(&seed, &acct_id_0, &acct_id_1);
    float amount = 5.0;

    /* Read from checking table */
    smallbank_checking_key_t chk_key_0;
    chk_key_0.acct_id = acct_id_0;
    auto chk_obj_0 = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key_0.item_key);
    dtx->AddToReadWriteSet(chk_obj_0);

    /* Read from checking account for acct_id_1 */
    smallbank_checking_key_t chk_key_1;
    chk_key_1.acct_id = acct_id_1;
    auto chk_obj_1 = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key_1.item_key);
    dtx->AddToReadWriteSet(chk_obj_1);

    if (!dtx->TxExe()) return false;

    /* if we are here, execution succeeded and we have locks */
    smallbank_checking_val_t *chk_val_0 = (smallbank_checking_val_t *) chk_obj_0->value;
    smallbank_checking_val_t *chk_val_1 = (smallbank_checking_val_t *) chk_obj_1->value;
    assert(chk_val_0->magic == smallbank_checking_magic);
    assert(chk_val_1->magic == smallbank_checking_magic);

    if (chk_val_0->bal < amount) {
        dtx->TxAbortReadWrite();
        return false;
    }

    chk_val_0->bal -= amount; /* Debit */
    chk_val_1->bal += amount; /* Credit */

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

/* Add $20 to acct_id's saving's account */
bool TxTransactSaving(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    /* Transaction parameters */
    uint64_t acct_id;
    smallbank_client->get_account(&seed, &acct_id);
    float amount = 20.20;

    /* Read from saving table */
    smallbank_savings_key_t sav_key;
    sav_key.acct_id = acct_id;
    auto sav_obj = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kSavingsTable, sav_key.item_key);
    dtx->AddToReadWriteSet(sav_obj);
    if (!dtx->TxExe()) return false;

    /* If we are here, execution succeeded and we have a lock */
    smallbank_savings_val_t *sav_val = (smallbank_savings_val_t *) sav_obj->value;
    assert(sav_val->magic == smallbank_savings_magic);
    sav_val->bal += amount; /* Update saving kBalance */
    bool commit_status = dtx->TxCommit();
    return commit_status;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool TxWriteCheck(tx_id_t tx_id, DTX *dtx) {
    dtx->TxBegin(tx_id);

    /* Transaction parameters */
    uint64_t acct_id;
    smallbank_client->get_account(&seed, &acct_id);
    float amount = 5.0;

    /* Read from savings. Read checking record for update. */
    smallbank_savings_key_t sav_key;
    sav_key.acct_id = acct_id;
    auto sav_obj = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kSavingsTable, sav_key.item_key);
    dtx->AddToReadOnlySet(sav_obj);

    smallbank_checking_key_t chk_key;
    chk_key.acct_id = acct_id;
    auto chk_obj = std::make_shared<DataItem>((table_id_t) SmallBankTableType::kCheckingTable, chk_key.item_key);
    dtx->AddToReadWriteSet(chk_obj);

    if (!dtx->TxExe()) return false;

    smallbank_savings_val_t *sav_val = (smallbank_savings_val_t *) sav_obj->value;
    smallbank_checking_val_t *chk_val = (smallbank_checking_val_t *) chk_obj->value;
    assert(sav_val->magic == smallbank_savings_magic);
    assert(chk_val->magic == smallbank_checking_magic);

    if (sav_val->bal + chk_val->bal < amount) {
        chk_val->bal -= (amount + 1);
    } else {
        chk_val->bal -= amount;
    }

    bool commit_status = dtx->TxCommit();
    return commit_status;
}

thread_local int running_tasks;

void WarmUp(DTXContext *context) {
    DTX *dtx = new DTX(context);
    bool tx_committed = false;
    for (int i = 0; i < 50000; ++i) {
        SmallBankTxType tx_type = workgen_arr[FastRand(&seed) % 100];
        uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
        switch (tx_type) {
            case SmallBankTxType::kAmalgamate:
                tx_committed = TxAmalgamate(iter, dtx);
                break;
            case SmallBankTxType::kBalance:
                tx_committed = TxBalance(iter, dtx);
                break;
            case SmallBankTxType::kDepositChecking:
                tx_committed = TxDepositChecking(iter, dtx);
                break;
            case SmallBankTxType::kSendPayment:
                tx_committed = TxSendPayment(iter, dtx);
                break;
            case SmallBankTxType::kTransactSaving:
                tx_committed = TxTransactSaving(iter, dtx);
                break;
            case SmallBankTxType::kWriteCheck:
                tx_committed = TxWriteCheck(iter, dtx);
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
        SmallBankTxType tx_type = workgen_arr[FastRand(&seed) % 100];
        uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
        attempt_tx++;
        clock_gettime(CLOCK_REALTIME, &tx_start_time);
#ifdef ABORT_DISCARD
        switch (tx_type) {
            case SmallBankTxType::kAmalgamate:
                do {
                    tx_committed = TxAmalgamate(iter, dtx);
                } while (!tx_committed);
                break;
            case SmallBankTxType::kBalance:
                do {
                    tx_committed = TxBalance(iter, dtx);
                } while (!tx_committed);
                break;
            case SmallBankTxType::kDepositChecking:
                do {
                    tx_committed = TxDepositChecking(iter, dtx);
                } while (!tx_committed);
                break;
            case SmallBankTxType::kSendPayment:
                do {
                    tx_committed = TxSendPayment(iter, dtx);
                } while (!tx_committed);
                break;
            case SmallBankTxType::kTransactSaving:
                do {
                    tx_committed = TxTransactSaving(iter, dtx);
                } while (!tx_committed);
                break;
            case SmallBankTxType::kWriteCheck:
                do {
                    tx_committed = TxWriteCheck(iter, dtx);
                } while (!tx_committed);
                break;
            default:
                printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
                abort();
        }
#else
        switch (tx_type) {
            case SmallBankTxType::kAmalgamate:
                tx_committed = TxAmalgamate(iter, dtx);
                break;
            case SmallBankTxType::kBalance:
                tx_committed = TxBalance(iter, dtx);
                break;
            case SmallBankTxType::kDepositChecking:
                tx_committed = TxDepositChecking(iter, dtx);
                break;
            case SmallBankTxType::kSendPayment:
                tx_committed = TxSendPayment(iter, dtx);
                break;
            case SmallBankTxType::kTransactSaving:
                tx_committed = TxTransactSaving(iter, dtx);
                break;
            case SmallBankTxType::kWriteCheck:
                tx_committed = TxWriteCheck(iter, dtx);
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
    ATTEMPTED_NUM = kMaxTransactions / threads / coroutines;
    auto hostname = GetHostName();
    seed = MurmurHash3_x86_32(hostname.c_str(), hostname.length(), 0xcc9e2d51) * kMaxThreads + id;
    smallbank_client = new SmallBank();
    workgen_arr = smallbank_client->CreateWorkgenArray();
    WarmUp(context);
    TaskPool::Enable();

    auto &task_pool = TaskPool::Get();
    running_tasks = coroutines;

    pthread_barrier_wait(&barrier);
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
    smallbank_client = new SmallBank();
    timespec ts_begin, ts_end;
    pthread_barrier_init(&barrier, nullptr, threads + 1);
    std::vector<std::thread> workers;
    workers.resize(threads);
    synchronize_begin(context);
    for (int i = 0; i < threads; ++i) {
        workers[i] = std::thread(execute_thread, i, context);
    }
    pthread_barrier_wait(&barrier);
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
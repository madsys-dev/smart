// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD
//
// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2021

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "../memstore.h"
#include "util/json_config.h"

static ALWAYS_INLINE uint32_t FastRand(uint64_t *seed) {
    *seed = *seed * 1103515245 + 12345;
    return (uint32_t) (*seed >> 32);
}

#define FREQUENCY_AMALGAMATE 15
#define FREQUENCY_BALANCE 15
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 25
#define FREQUENCY_TRANSACT_SAVINGS 15
#define FREQUENCY_WRITE_CHECK 15

#define TX_HOT 90

union smallbank_savings_key_t {
    uint64_t acct_id;
    uint64_t item_key;

    smallbank_savings_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(smallbank_savings_key_t) == sizeof(uint64_t), "");

struct smallbank_savings_val_t {
    uint32_t magic;
    float bal;
};
static_assert(sizeof(smallbank_savings_val_t) == sizeof(uint64_t), "");

union smallbank_checking_key_t {
    uint64_t acct_id;
    uint64_t item_key;

    smallbank_checking_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(smallbank_checking_key_t) == sizeof(uint64_t), "");

struct smallbank_checking_val_t {
    uint32_t magic;
    float bal;
};
static_assert(sizeof(smallbank_checking_val_t) == sizeof(uint64_t), "");

#define SmallBank_MAGIC             97
#define smallbank_savings_magic     (SmallBank_MAGIC)
#define smallbank_checking_magic    (SmallBank_MAGIC + 1)

#define SmallBank_TX_TYPES 6
enum class SmallBankTxType : int {
    kAmalgamate,
    kBalance,
    kDepositChecking,
    kSendPayment,
    kTransactSaving,
    kWriteCheck,
};

enum class SmallBankTableType : uint64_t {
    kSavingsTable = 1,
    kCheckingTable,
};

class SmallBank {
public:
    uint32_t num_accounts_global, num_hot_global;
    HashStore *savings_table;
    HashStore *checking_table;
    std::vector<HashStore *> table_ptrs;

    SmallBank() {
        std::string path = ROOT_DIR "/config/transaction.json";
        auto json_config = JsonConfig::load_file(path);
        auto conf = json_config.get("smallbank");
        num_accounts_global = conf.get("num_accounts").get_uint64();
        num_hot_global = conf.get("num_hot_accounts").get_uint64();
        assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);
        savings_table = nullptr;
        checking_table = nullptr;
    }

    ~SmallBank() {
        delete savings_table;
        delete checking_table;
    }

    SmallBankTxType *CreateWorkgenArray() {
        SmallBankTxType *workgen_arr = new SmallBankTxType[100];
        int i = 0, j = 0;
        j += FREQUENCY_AMALGAMATE;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kAmalgamate;
        j += FREQUENCY_BALANCE;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kBalance;
        j += FREQUENCY_DEPOSIT_CHECKING;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kDepositChecking;
        j += FREQUENCY_SEND_PAYMENT;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kSendPayment;
        j += FREQUENCY_TRANSACT_SAVINGS;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kTransactSaving;
        j += FREQUENCY_WRITE_CHECK;
        for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kWriteCheck;
        assert(i == 100 && j == 100);
        return workgen_arr;
    }

    inline void get_account(uint64_t *seed, uint64_t *acct_id) const {
        if (FastRand(seed) % 100 < TX_HOT) {
            *acct_id = FastRand(seed) % num_hot_global;
        } else {
            *acct_id = FastRand(seed) % num_accounts_global;
        }
    }

    inline void get_two_accounts(uint64_t *seed, uint64_t *acct_id_0, uint64_t *acct_id_1) const {
        if (FastRand(seed) % 100 < TX_HOT) {
            *acct_id_0 = FastRand(seed) % num_hot_global;
            *acct_id_1 = FastRand(seed) % num_hot_global;
            while (*acct_id_1 == *acct_id_0) {
                *acct_id_1 = FastRand(seed) % num_hot_global;
            }
        } else {
            *acct_id_0 = FastRand(seed) % num_accounts_global;
            *acct_id_1 = FastRand(seed) % num_accounts_global;
            while (*acct_id_1 == *acct_id_0) {
                *acct_id_1 = FastRand(seed) % num_accounts_global;
            }
        }
    }

    void LoadTable(MemStoreAllocParam *mem_store_alloc_param, MemStoreReserveParam *mem_store_reserve_param) {
        const static size_t kSavingTableItemCount = 200000;
        const static size_t kCheckingTableItemCount = 200000;

        savings_table = new HashStore((table_id_t) SmallBankTableType::kSavingsTable,
                                      kSavingTableItemCount, mem_store_alloc_param);
        PopulateSavingsTable(mem_store_reserve_param);
        table_ptrs.push_back(savings_table);

        checking_table = new HashStore((table_id_t) SmallBankTableType::kCheckingTable,
                                       kCheckingTableItemCount, mem_store_alloc_param);
        PopulateCheckingTable(mem_store_reserve_param);
        table_ptrs.push_back(checking_table);
    }

    void PopulateSavingsTable(MemStoreReserveParam *mem_store_reserve_param) {
        for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
            smallbank_savings_key_t savings_key;
            savings_key.acct_id = (uint64_t) acct_id;
            smallbank_savings_val_t savings_val;
            savings_val.magic = smallbank_savings_magic;
            savings_val.bal = 1000000000ull;
            LoadRecord(savings_table, savings_key.item_key,
                       (void *) &savings_val, sizeof(smallbank_savings_val_t),
                       (table_id_t) SmallBankTableType::kSavingsTable,
                       mem_store_reserve_param);
        }
    }

    void PopulateCheckingTable(MemStoreReserveParam *mem_store_reserve_param) {
        for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
            smallbank_checking_key_t checking_key;
            checking_key.acct_id = (uint64_t) acct_id;
            smallbank_checking_val_t checking_val;
            checking_val.magic = smallbank_checking_magic;
            checking_val.bal = 1000000000ull;
            LoadRecord(checking_table, checking_key.item_key,
                       (void *) &checking_val, sizeof(smallbank_checking_val_t),
                       (table_id_t) SmallBankTableType::kCheckingTable,
                       mem_store_reserve_param);
        }
    }

    int LoadRecord(HashStore *table, itemkey_t item_key, void *val_ptr, size_t val_size, table_id_t table_id,
                   MemStoreReserveParam *mem_store_reserve_param) {
        assert(val_size <= MAX_ITEM_SIZE);
        DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t *) val_ptr);
        DataItem *inserted_item = table->LocalInsert(item_key, item_to_be_inserted, mem_store_reserve_param);
        inserted_item->remote_offset = table->GetItemRemoteOffset(inserted_item);
        return 1;
    }

    ALWAYS_INLINE
    std::vector<HashStore *> GetHashStore() {
        return table_ptrs;
    }
};


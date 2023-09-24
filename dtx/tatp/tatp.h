// Some contents of this file are derived from FORD 
// https://github.com/minghust/FORD
//
// Author: Ming Zhang
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

/*
 * Up to 1 billion subscribers so that FastGetSubscribeNumFromSubscribeID() requires
 * only 3 modulo operations.
 */
#define TATP_MAX_SUBSCRIBERS 1000000000

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
#define FREQUENCY_GET_SUBSCRIBER_DATA 35    // Single
#define FREQUENCY_GET_ACCESS_DATA 35        // Single
#define FREQUENCY_GET_NEW_DESTINATION 10    // Single
#define FREQUENCY_UPDATE_SUBSCRIBER_DATA 2  // Single
#define FREQUENCY_UPDATE_LOCATION 14        // Multi
#define FREQUENCY_INSERT_CALL_FORWARDING 2  // Multi
#define FREQUENCY_DELETE_CALL_FORWARDING 2  // Multi

/******************** TATP table definitions (Schemas of key and value) start **********************/
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/* A 64-bit encoding for 15-character decimal strings. */
union tatp_sub_number_t {
    struct {
        uint32_t dec_0: 4;
        uint32_t dec_1: 4;
        uint32_t dec_2: 4;
        uint32_t dec_3: 4;
        uint32_t dec_4: 4;
        uint32_t dec_5: 4;
        uint32_t dec_6: 4;
        uint32_t dec_7: 4;
        uint32_t dec_8: 4;
        uint32_t dec_9: 4;
        uint32_t dec_10: 4;
        uint32_t dec_11: 4;
        uint32_t dec_12: 4;
        uint32_t dec_13: 4;
        uint32_t dec_14: 4;
        uint32_t dec_15: 4;
    };

    struct {
        uint64_t dec_0_1_2: 12;
        uint64_t dec_3_4_5: 12;
        uint64_t dec_6_7_8: 12;
        uint64_t dec_9_10_11: 12;
        uint64_t unused: 16;
    };

    itemkey_t item_key;
};
static_assert(sizeof(tatp_sub_number_t) == sizeof(itemkey_t), "");

/*
 * SUBSCRIBER table
 * Primary key: <uint32_t s_id>
 * Value size: 40 bytes. Full value read in GET_SUBSCRIBER_DATA.
 */
union tatp_sub_key_t {
    struct {
        uint32_t s_id;
        uint8_t unused[4];
    };
    itemkey_t item_key;

    tatp_sub_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(tatp_sub_key_t) == sizeof(itemkey_t), "");

struct tatp_sub_val_t {
    tatp_sub_number_t sub_number;
    char sub_number_unused[7]; /* sub_number should be 15 bytes. We used 8 above. */
    char hex[5];
    char bytes[10];
    short bits;
    uint32_t msc_location;
    uint32_t vlr_location;
};
static_assert(sizeof(tatp_sub_val_t) == 40, "");

/*
 * Secondary SUBSCRIBER table
 * Key: <tatp_sub_number_t>
 * Value size: 8 bytes
 */
union tatp_sec_sub_key_t {
    tatp_sub_number_t sub_number;
    itemkey_t item_key;

    tatp_sec_sub_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(tatp_sec_sub_key_t) == sizeof(itemkey_t), "");

struct tatp_sec_sub_val_t {
    uint32_t s_id;
    uint8_t magic;
    uint8_t unused[3];
};
static_assert(sizeof(tatp_sec_sub_val_t) == 8, "");

/*
 * ACCESS INFO table
 * Primary key: <uint32_t s_id, uint8_t ai_type>
 * Value size: 16 bytes
 */
union tatp_accinf_key_t {
    struct {
        uint32_t s_id;
        uint8_t ai_type;
        uint8_t unused[3];
    };
    itemkey_t item_key;

    tatp_accinf_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(tatp_accinf_key_t) == sizeof(itemkey_t), "");

struct tatp_accinf_val_t {
    char data1;
    char data2;
    char data3[3];
    char data4[5];
    uint8_t unused[6];
};
static_assert(sizeof(tatp_accinf_val_t) == 16, "");

/*
 * SPECIAL FACILITY table
 * Primary key: <uint32_t s_id, uint8_t sf_type>
 * Value size: 8 bytes
 */
union tatp_specfac_key_t {
    struct {
        uint32_t s_id;
        uint8_t sf_type;
        uint8_t unused[3];
    };
    itemkey_t item_key;

    tatp_specfac_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(tatp_specfac_key_t) == sizeof(itemkey_t), "");

struct tatp_specfac_val_t {
    char is_active;
    char error_cntl;
    char data_a;
    char data_b[5];
};
static_assert(sizeof(tatp_specfac_val_t) == 8, "");

/*
 * CALL FORWARDING table
 * Primary key: <uint32_t s_id, uint8_t sf_type, uint8_t start_time>
 * Value size: 16 bytes
 */
union tatp_callfwd_key_t {
    struct {
        uint32_t s_id;
        uint8_t sf_type;
        uint8_t start_time;
        uint8_t unused[2];
    };
    itemkey_t item_key;

    tatp_callfwd_key_t() {
        item_key = 0;
    }
};

static_assert(sizeof(tatp_callfwd_key_t) == sizeof(itemkey_t), "");

struct tatp_callfwd_val_t {
    uint8_t end_time;
    char numberx[15];
};
static_assert(sizeof(tatp_callfwd_val_t) == 16, "");

/******************** TATP table definitions (Schemas of key and value) end **********************/

// Magic numbers for debugging. These are unused in the spec.
#define TATP_MAGIC 97 /* Some magic number <= 255 */
#define tatp_sub_msc_location_magic (TATP_MAGIC)
#define tatp_sec_sub_magic (TATP_MAGIC + 1)
#define tatp_accinf_data1_magic (TATP_MAGIC + 2)
#define tatp_specfac_data_b0_magic (TATP_MAGIC + 3)
#define tatp_callfwd_numberx0_magic (TATP_MAGIC + 4)

// Transaction workload type
#define TATP_TX_TYPES 7
enum class TATPTxType {
    kGetSubsciberData = 0,
    kGetAccessData,
    kGetNewDestination,
    kUpdateSubscriberData,
    kUpdateLocation,
    kInsertCallForwarding,
    kDeleteCallForwarding,
};

// Table id
enum class TATPTableType : uint64_t {
    kSubscriberTable = 1,
    kSecSubscriberTable,
    kSpecialFacilityTable,
    kAccessInfoTable,
    kCallForwardingTable,
};

class TATP {
public:
    /* Map 0--999 to 12b, 4b/digit decimal representation */
    uint16_t *map_1000;

    uint32_t subscriber_size;

    /* TATP spec parameter for non-uniform random generation */
    uint32_t A;

    /* Tables */
    HashStore *subscriber_table;

    HashStore *sec_subscriber_table;

    HashStore *special_facility_table;

    HashStore *access_info_table;

    HashStore *call_forwarding_table;

    std::vector<HashStore *> table_ptrs;

    // For server and client usage: Provide interfaces to servers for loading tables
    TATP() {
        /* Init the precomputed decimal map */
        map_1000 = (uint16_t *) malloc(1000 * sizeof(uint16_t));
        for (size_t i = 0; i < 1000; i++) {
            uint32_t dig_1 = (i / 1) % 10;
            uint32_t dig_2 = (i / 10) % 10;
            uint32_t dig_3 = (i / 100) % 10;
            map_1000[i] = (dig_3 << 8) | (dig_2 << 4) | dig_1;
        }

        std::string path = ROOT_DIR "/config/transaction.json";
        auto json_config = JsonConfig::load_file(path);
        auto conf = json_config.get("tatp");
        subscriber_size = conf.get("num_subscriber").get_uint64();

        assert(subscriber_size <= TATP_MAX_SUBSCRIBERS);
        /* Compute the "A" parameter for nurand distribution as per spec */
        if (subscriber_size <= 1000000) {
            A = 65535;
        } else if (subscriber_size <= 10000000) {
            A = 1048575;
        } else {
            A = 2097151;
        }

        subscriber_table = nullptr;
        sec_subscriber_table = nullptr;
        special_facility_table = nullptr;
        access_info_table = nullptr;
        call_forwarding_table = nullptr;
    }

    ~TATP() {
        if (subscriber_table) delete subscriber_table;
        if (sec_subscriber_table) delete sec_subscriber_table;
        if (special_facility_table) delete special_facility_table;
        if (access_info_table) delete access_info_table;
        if (call_forwarding_table) delete call_forwarding_table;
    }

    /* create workload generation array for benchmarking */
    ALWAYS_INLINE
    TATPTxType *CreateWorkgenArray() {
        TATPTxType *workgen_arr = new TATPTxType[100];

        int i = 0, j = 0;

        j += FREQUENCY_GET_SUBSCRIBER_DATA;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetSubsciberData;

        j += FREQUENCY_GET_ACCESS_DATA;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetAccessData;

        j += FREQUENCY_GET_NEW_DESTINATION;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kGetNewDestination;

        j += FREQUENCY_UPDATE_SUBSCRIBER_DATA;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kUpdateSubscriberData;

        j += FREQUENCY_UPDATE_LOCATION;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kUpdateLocation;

        j += FREQUENCY_INSERT_CALL_FORWARDING;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kInsertCallForwarding;

        j += FREQUENCY_DELETE_CALL_FORWARDING;
        for (; i < j; i++) workgen_arr[i] = TATPTxType::kDeleteCallForwarding;

        assert(i == 100 && j == 100);
        return workgen_arr;
    }

    /*
       * Get a non-uniform-random distributed subscriber ID according to spec.
       * To get a non-uniformly random number between 0 and y:
       * NURand(A, 0, y) = (get_random(0, A) | get_random(0, y)) % (y + 1)
       */
    ALWAYS_INLINE
    uint32_t GetNonUniformRandomSubscriber(uint64_t *thread_local_seed) const {
        return ((FastRand(thread_local_seed) % subscriber_size) |
                (FastRand(thread_local_seed) & A)) %
               subscriber_size;
    }

    /* Get a subscriber number from a subscriber ID, fast */
    ALWAYS_INLINE
    tatp_sub_number_t FastGetSubscribeNumFromSubscribeID(uint32_t s_id) const {
        tatp_sub_number_t sub_number;
        sub_number.item_key = 0;
        sub_number.dec_0_1_2 = map_1000[s_id % 1000];
        s_id /= 1000;
        sub_number.dec_3_4_5 = map_1000[s_id % 1000];
        s_id /= 1000;
        sub_number.dec_6_7_8 = map_1000[s_id % 1000];

        return sub_number;
    }

    /* Get a subscriber number from a subscriber ID, simple */
    tatp_sub_number_t SimpleGetSubscribeNumFromSubscribeID(uint32_t s_id) {
#define update_sid()     \
  do {                   \
    s_id = s_id / 10;    \
    if (s_id == 0) {     \
      return sub_number; \
    }                    \
  } while (false)

        tatp_sub_number_t sub_number;
        sub_number.item_key = 0; /* Zero out all digits */

        sub_number.dec_0 = s_id % 10;
        update_sid();

        sub_number.dec_1 = s_id % 10;
        update_sid();

        sub_number.dec_2 = s_id % 10;
        update_sid();

        sub_number.dec_3 = s_id % 10;
        update_sid();

        sub_number.dec_4 = s_id % 10;
        update_sid();

        sub_number.dec_5 = s_id % 10;
        update_sid();

        sub_number.dec_6 = s_id % 10;
        update_sid();

        sub_number.dec_7 = s_id % 10;
        update_sid();

        sub_number.dec_8 = s_id % 10;
        update_sid();

        sub_number.dec_9 = s_id % 10;
        update_sid();

        sub_number.dec_10 = s_id % 10;
        update_sid();

        assert(s_id == 0);
        return sub_number;
    }

    // For server-side usage
    void LoadTable(MemStoreAllocParam *mem_store_alloc_param,
                   MemStoreReserveParam *mem_store_reserve_param);

    void PopulateSubscriberTable(MemStoreReserveParam *mem_store_reserve_param);

    void PopulateSecondarySubscriberTable(MemStoreReserveParam *mem_store_reserve_param);

    void PopulateAccessInfoTable(MemStoreReserveParam *mem_store_reserve_param);

    void PopulateSpecfacAndCallfwdTable(MemStoreReserveParam *mem_store_reserve_param);

    int LoadRecord(HashStore *table,
                   itemkey_t item_key,
                   void *val_ptr,
                   size_t val_size,
                   table_id_t table_id,
                   MemStoreReserveParam *mem_store_reserve_param);

    std::vector<uint8_t> SelectUniqueItem(uint64_t *tmp_seed, std::vector<uint8_t> values, unsigned N, unsigned M);

    ALWAYS_INLINE
    std::vector<HashStore *> GetHashStore() {
        return table_ptrs;
    }
};

void TATP::LoadTable(MemStoreAllocParam *mem_store_alloc_param,
                     MemStoreReserveParam *mem_store_reserve_param) {
    const static size_t SubscriberItemCount = 12500;

    subscriber_table = new HashStore((table_id_t) TATPTableType::kSubscriberTable, SubscriberItemCount,
                                     mem_store_alloc_param);
    PopulateSubscriberTable(mem_store_reserve_param);
    table_ptrs.push_back(subscriber_table);

    sec_subscriber_table = new HashStore((table_id_t) TATPTableType::kSecSubscriberTable, SubscriberItemCount,
                                         mem_store_alloc_param);
    PopulateSecondarySubscriberTable(mem_store_reserve_param);
    table_ptrs.push_back(sec_subscriber_table);

    const static size_t AccessInfoItemCount = 40000;
    access_info_table = new HashStore((table_id_t) TATPTableType::kAccessInfoTable, AccessInfoItemCount,
                                      mem_store_alloc_param);
    PopulateAccessInfoTable(mem_store_reserve_param);
    table_ptrs.push_back(access_info_table);

    const static size_t SpecialFacilityItemCount = 40000;
    const static size_t CallForwardingItemCount = 50000;

    special_facility_table = new HashStore((table_id_t) TATPTableType::kSpecialFacilityTable,
                                           SpecialFacilityItemCount, mem_store_alloc_param);
    call_forwarding_table = new HashStore((table_id_t) TATPTableType::kCallForwardingTable,
                                          CallForwardingItemCount, mem_store_alloc_param);
    PopulateSpecfacAndCallfwdTable(mem_store_reserve_param);
    table_ptrs.push_back(special_facility_table);
    table_ptrs.push_back(call_forwarding_table);
}

void TATP::PopulateSubscriberTable(MemStoreReserveParam *mem_store_reserve_param) {
    /* All threads must execute the loop below deterministically */
    uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */
    int total_records_inserted = 0, total_records_examined = 0;

    /* Populate the table */
    for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
        tatp_sub_key_t key;
        key.s_id = s_id;

        /* Initialize the subscriber payload */
        tatp_sub_val_t sub_val;
        sub_val.sub_number = SimpleGetSubscribeNumFromSubscribeID(s_id);

        for (int i = 0; i < 5; i++) {
            sub_val.hex[i] = FastRand(&tmp_seed);
        }

        for (int i = 0; i < 10; i++) {
            sub_val.bytes[i] = FastRand(&tmp_seed);
        }

        sub_val.bits = FastRand(&tmp_seed);
        sub_val.msc_location = tatp_sub_msc_location_magic; /* Debug */
        sub_val.vlr_location = FastRand(&tmp_seed);

        total_records_inserted += LoadRecord(
                subscriber_table,
                key.item_key,
                (void *) &sub_val,
                sizeof(tatp_sub_val_t),
                (table_id_t) TATPTableType::kSubscriberTable,
                mem_store_reserve_param);
        total_records_examined++;
        //std::cerr << "total_records_examined: " << total_records_examined << std::endl;
    }
}

void TATP::PopulateSecondarySubscriberTable(MemStoreReserveParam *mem_store_reserve_param) {
    int total_records_inserted = 0, total_records_examined = 0;

    /* Populate the tables */
    for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
        tatp_sec_sub_key_t key;
        key.sub_number = SimpleGetSubscribeNumFromSubscribeID(s_id);

        /* Initialize the subscriber payload */
        tatp_sec_sub_val_t sec_sub_val;
        sec_sub_val.s_id = s_id;
        sec_sub_val.magic = tatp_sec_sub_magic;

        total_records_inserted += LoadRecord(
                sec_subscriber_table, key.item_key,
                (void *) &sec_sub_val, sizeof(tatp_sec_sub_val_t),
                (table_id_t) TATPTableType::kSecSubscriberTable,
                mem_store_reserve_param);
        total_records_examined++;
    }
}

void TATP::PopulateAccessInfoTable(MemStoreReserveParam *mem_store_reserve_param) {
    std::vector<uint8_t> ai_type_values = {1, 2, 3, 4};

    /* All threads must execute the loop below deterministically */
    uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */
    int total_records_inserted = 0, total_records_examined = 0;

    /* Populate the table */
    for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
        std::vector<uint8_t> ai_type_vec = SelectUniqueItem(&tmp_seed, ai_type_values, 1, 4);
        for (uint8_t ai_type: ai_type_vec) {
            /* Insert access info record */
            tatp_accinf_key_t key;
            key.s_id = s_id;
            key.ai_type = ai_type;

            tatp_accinf_val_t accinf_val;
            accinf_val.data1 = tatp_accinf_data1_magic;

            /* Insert into table if I am replica number repl_i for key */
            total_records_inserted += LoadRecord(
                    access_info_table, key.item_key,
                    (void *) &accinf_val, sizeof(tatp_accinf_val_t),
                    (table_id_t) TATPTableType::kAccessInfoTable,
                    mem_store_reserve_param);
            total_records_examined++;
        }
    }
}

/*
 * Which rows are inserted into the CALL FORWARDING table depends on which
 * rows get inserted into the SPECIAL FACILITY, so process these two jointly.
 */
void TATP::PopulateSpecfacAndCallfwdTable(MemStoreReserveParam *mem_store_reserve_param) {
    std::vector<uint8_t> sf_type_values = {1, 2, 3, 4};
    std::vector<uint8_t> start_time_values = {0, 8, 16};

    int total_records_inserted = 0, total_records_examined = 0;

    /* All threads must execute the loop below deterministically */
    uint64_t tmp_seed = 0xdeadbeef; /* Temporary seed for this function only */

    /* Populate the tables */
    for (uint32_t s_id = 0; s_id < subscriber_size; s_id++) {
        std::vector<uint8_t> sf_type_vec = SelectUniqueItem(
                &tmp_seed, sf_type_values, 1, 4);

        for (uint8_t sf_type: sf_type_vec) {
            /* Insert the special facility record */
            tatp_specfac_key_t key;
            key.s_id = s_id;
            key.sf_type = sf_type;

            tatp_specfac_val_t specfac_val;
            specfac_val.data_b[0] = tatp_specfac_data_b0_magic;
            specfac_val.is_active = (FastRand(&tmp_seed) % 100 < 85) ? 1 : 0;
            total_records_inserted += LoadRecord(
                    special_facility_table, key.item_key,
                    (void *) &specfac_val, sizeof(tatp_specfac_val_t),
                    (table_id_t) TATPTableType::kSpecialFacilityTable,
                    mem_store_reserve_param);
            total_records_examined++;

            /*
                   * The TATP spec requires a different initial probability
                   * distribution of Call Forwarding records (see README). Here, we
                   * populate the table using the steady state distribution.
                   */
            for (size_t start_time = 0; start_time <= 16; start_time += 8) {
                /*
                         * At steady state, each @start_time for <s_id, sf_type> is
                         * equally likely to be present or absent.
                         */
                if (FastRand(&tmp_seed) % 2 == 0) {
                    continue;
                }

                /* Insert the call forwarding record */
                tatp_callfwd_key_t key;
                key.s_id = s_id;
                key.sf_type = sf_type;
                key.start_time = start_time;

                tatp_callfwd_val_t callfwd_val;
                callfwd_val.numberx[0] = tatp_callfwd_numberx0_magic;
                /* At steady state, @end_time is unrelated to @start_time */
                callfwd_val.end_time = (FastRand(&tmp_seed) % 24) + 1;
                total_records_inserted += LoadRecord(
                        call_forwarding_table, key.item_key,
                        (void *) &callfwd_val, sizeof(tatp_callfwd_val_t),
                        (table_id_t) TATPTableType::kCallForwardingTable,
                        mem_store_reserve_param);
                total_records_examined++;

            } /* End loop start_time */
        }   /* End loop sf_type */
    }     /* End loop s_id */
}

int TATP::LoadRecord(HashStore *table,
                     itemkey_t item_key,
                     void *val_ptr,
                     size_t val_size,
                     table_id_t table_id,
                     MemStoreReserveParam *mem_store_reserve_param) {
    assert(val_size <= MAX_ITEM_SIZE);
    /* Insert into HashStore */
    DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t *) val_ptr);
    DataItem *inserted_item = table->LocalInsert(item_key, item_to_be_inserted, mem_store_reserve_param);
    inserted_item->remote_offset = table->GetItemRemoteOffset(inserted_item);
    return 1;
}

/*
 * Select between N and M unique items from the values vector. The number
 * of values to be selected, and the actual values are chosen at random.
 */
std::vector<uint8_t> TATP::SelectUniqueItem(uint64_t *tmp_seed, std::vector<uint8_t> values, unsigned N, unsigned M) {
    assert(M >= N);
    assert(M >= values.size());

    std::vector<uint8_t> ret;

    int used[32];
    memset(used, 0, 32 * sizeof(int));

    int to_select = (FastRand(tmp_seed) % (M - N + 1)) + N;
    for (int i = 0; i < to_select; i++) {
        int index = FastRand(tmp_seed) % values.size();
        uint8_t value = values[index];
        assert(value < 32);

        if (used[value] == 1) {
            i--;
            continue;
        }

        used[value] = 1;
        ret.push_back(value);
    }
    return ret;
}
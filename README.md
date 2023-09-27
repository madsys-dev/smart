# SMART

SMART is a framework to deal with the complexity of building scalable applications for memory disaggregation, which integrated with the following techniques to resolve RDMA-related performance issues:
- Thread-aware resource allocation,
- Adaptive work request throttling, and
- Conflict avoidance technique.

To ease the programming, SMART provides a set of coroutine-based asynchronous APIs that are almost identical to the original RDMA verbs. We also design adaptive methods for adjusting configuration parameters that are important to the performance.

For more technical details, please refer to our paper: 

> Feng Ren, Mingxing Zhang, Kang Chen, Huaxia Xia, Zuoning Chen, Yongwei Wu. Scaling Up Memory Disaggregated Applications with SMART. In 29th ACM International Conference on Architectural Support for Programming Languages and Operating Systems (ASPLOS '24).

## Artifact Contents
- SMART’s library (including techniques mentioned in this paper, `include/` and `smart/`);
- Synthesis workloads (`test/`)
- Applications: SMART-HT (derived from RACE, `hashtable/`), SMART-BT (derived from Sherman, `btree/`) and SMART-DTX (derived from FORD, `dtx/`).
- Reproduce scripts (`ae/`)

## System Requirements
* **Mellanox InfiniBand RNIC** (ConnectX-6 in our testbed). Mellanox OpenFabrics Enterprise Distribution for Linux (MLNX_OFED) v5.3-1.0.0.1. Other OFED versions are under testing.

* **Optane DC PMEM (optional)**, configured as DEVDAX mode and mounted to `/dev/dax1.0`. Optane DC PMEM is used for evaulating distributed persistent transactions (`dtx/`) in our paper. However, it is okay to perform these tests using DRAM only. We provide an option `dev_dax_path` in configuration file `config/backend.json`, that specifies the PMEM DEVDAX device (e.g. `/dev/dax1.0`) to be used. By default, the value of `dev_dax_path` is an empty string, which indicates that only DRAM is used. If PMEM exists, Run command `sudo chmod 777 /dev/dax1.0` that allows unprevileged users to manipulate PMEM data.

* **Build Toolchain**: GCC = 9.4.0 or newer, CMake = 3.16.3 or newer.

* **Other Software**: libnuma, clustershell, Python 3 (with matplotlib, numpy, python-tk)

<details>
  <summary>Evaluation setup in this paper</summary>
  
  ### Hardware dependencies
  - **CPU**: Two-socket Intel Xeon Gold 6240R CPU (96 cores in total)
  - **Memory**: 384GB DDR4 DRAM (2666MHz)
  - **Persistent Memory**: 1.5TB (128GB*12) Intel Optane DC Persistent Memory (1st Gen) with DEVDAX mode
  - **RNIC**: 200Gbps Mellanox ConnectX-6 InfiniBand RNIC. Each RNIC is connected to a 200 Gbps Mellanox InfiniBand switch

  ### Software dependencies
  - **RNIC Driver**: Mellanox OpenFabrics Enterprise Distribution for Linux (MLNX_OFED) v5.x (v5.3-1.0.0.1 in this paper).
</details>

## Build & Install

### Build `SMART`
Execute the following command in your terminal:
```bash
bash ./deps.sh
bash ./build.sh
```

### Patch the RDMA Core Library
We provide a patched RDMA core library that unlimits the number of median-latency doorbell registers per RDMA context. However, if the installed version of MLNX_OFED is **NOT** v5.3-1.0.0.1, you must patch the library by yourself, and replace the `build/libmlx5.so` file. The guide is provided [here](patch/guide.md).

## Functional Evaluation: Basic Test
We provide a micro benchmark program `test/test_rdma` in SMART. It can be used to evaluate the throughput of RDMA READs/WRITEs between two servers. Note that the optimizations of SMART are enabled by default.

### Step 1: Setting Parameters

1. Set the server hostname, access granularity and type in `config/test_rdma.json`:
   ```json
   {
      "servers": [
         "optane06"     // the hostname of server side
      ],
      "port": 12345,    // TCP port for connection establishation
      "block_size": 8,  // access granularity in bytes
      "dump_file_path": "test_rdma.csv",  // path of dump file
      "type": "read"    // can be `read`, `write` or `atomic`
   }
   ```
2. Set RDMA device and enabled optimizations in `config/smart_config.json`:
   ```json
   {
      "infiniband": {
         "name": "",    // `mlx5_0` by default
         "port": 1
      },

      // available optimizations
      "use_thread_aware_alloc": true, 
      "thread_aware_alloc": {
         "total_uuar": 100,    // >= shared_uuar
         "shared_uuar": 96,    // >= thread count
         "shared_cq": true
      },

      "use_work_req_throt": true,
      "use_conflict_avoidance": true,
      "use_speculative_lookup": true,
   }
   ```

### Step 2: Run Server
In server side (`optane06` in the sample), run the following command:
```bash
cd /path/to/smart && cd build
LD_PRELOAD=libmlx5.so ./test/test_rdma
```

### Step 3: Run Client 
In another machine, run the following command:
```bash
cd /path/to/smart && cd build
LD_PRELOAD=libmlx5.so ./test/test_rdma \
   [nr_thread] \
   [outstanding_work_request_per_thread]
```
For example,
```bash
LD_PRELOAD=libmlx5.so ./test/test_rdma 96 8
```
### Result
After execution, the client displays the following information to stdout. 
```
rdma-read: #threads=96, #depth=8, #block\_size=8, BW=848.217 MB/s, IOPS=111.177 M/s, conn establish time=1245.924 ms
```
It also append a line to `test_rdma.csv` (specified by `"dump_file_path"`):
```
rdma-read, 96, 8, 8, 848.217, 111.177, 1245.924
```

## Functional Evaluation: Applications
For the sake of illustration, we present the functional evaluation method based on the following cluster. 
- **Memory Blades**: hostname `optane06` and `optane07`, with TCP port `12345`,
- **Compute Blades**: hostname `optane04`

### Configuration Files

SMART and its applications rely on multiple configuration files. By default, they are located in the `config/` subdirectory. You can use other files using environment variables `SMART_CONFIG_PATH` and `APP_CONFIG_PATH`.

#### 1. `smart_config.json`
Options of the SMART framework, which are appliable to all kinds of servers. See [here](#step-1-setting-parameters) in detail.
  
#### 2. `backend.json`
Options of backend servers running in memory blades. 
```json
{
  "dev_dax_path": "", // empty string for DRAM, /dev/daxX.Y for NVM
  "capacity": 16000,  // amount of memory to use (MiB)
  "tcp_port": 12345,  // Listen TCP port
  "nic_numa_node": 1  // Prefer bind socket
}
```

#### 3A. `datastructure.json`
Options of hashtable/B+tree clients running in the compute blades. Use `memory_servers` to specify **hostnames and ports** of memory blades to be connected. If you change the topology of the cluster, modify it accordingly.
```json
{
  "dataset": "ycsb-a",   // see "include/util/ycsb.h" for available datasets
  "dump_file_path": "datastructure.csv",

  "insert_before_execution": true, // insert all keys before performing tests
  "max_key": 100000000,  // key range [0, max_key)
  "key_length": 8,       // key length in bytes
  "value_length": 8,     // value length in bytes
  "rehash_key": false,   // key/value randomly inserted
  "duration": 15,        // test duration in seconds
  "zipfian_const": 0.99, // zipfian parameter

  "memory_servers": [    // memory server hostnames and ports
    {
      "hostname": "optane06",
      "port": 12345
    },
    {
      "hostname": "optane07",
      "port": 12345
    }
  ]
}
```
  
#### 3B. `transaction.json`
Options of transaction processing clients running in the compute blades. Use `memory_servers` to specify **hostnames and ports** of memory blades to be connected. If you change the topology of the cluster, modify it accordingly.
```json
{
  "memory_servers": [
    {
      "hostname": "optane06",
      "port": 12345
    },
    {
      "hostname": "optane07",
      "port": 12345
    }
  ],
  "tpcc": { ... },
  "smallbank": { ... },
  "tatp": { ... },
  "nr_transactions": 60000000,
  "dump_file_path": "dtx.csv"
}
```

### Start Memory Blades
For each memory blade **(`optane06` and `optane07` in our example)**, run one of the following programs:
   ```bash
   # working path should be build/

   # start hash table backend
   LD_PRELOAD=libmlx5.so ./hashtable/hashtable_backend 

   # start B+Tree backend
   LD_PRELOAD=libmlx5.so ./btree/btree_backend

   # start SmallBank backend
   LD_PRELOAD=libmlx5.so ./dtx/smallbank/smallbank_backend

   # start TATP backend
   LD_PRELOAD=libmlx5.so ./dtx/tatp/tatp_backend
   ```

### Start Compute Blades
In compute blades **(`optane04` in our example)**, run one of the benchmark program using the following command. It must match the backend started in the previous step. 

```bash
# working path should be build/

# start hash table bench
# `coro` means `coroutine`
LD_PRELOAD=libmlx5.so ./hashtable/hashtable_bench [nr_thread] [nr_coro] 

# start B+Tree bench
LD_PRELOAD=libmlx5.so ./btree/btree_bench [nr_thread] [nr_coro] 

# start SmallBank bench
LD_PRELOAD=libmlx5.so ./dtx/smallbank/smallbank_bench [nr_thread] [nr_coro] 

# start TATP bench
LD_PRELOAD=libmlx5.so ./dtx/tatp/tatp_bench [nr_thread] [nr_coro] 
```

After execution, the benchmark program displays the throughput and latency to stdout. It also adds a line to the file specified by `dump_file_path`.


## Reproduce Evaluation
We also provide scripts to reproduce all experiments in Section 3 and Section 6. In Section 3, there are 3 experiments (Figures 3, 4 and 5), each of them points out one of the scalability bottlenecks. In Section 6, there are 9 experiments (Figures 7–14, and Table 1) that compare SMART-refactorized applications (i.e. SMART-HT, SMART-BT and SMART-DTX) with the state-of-the-art disaggregated systems (i.e. RACE, Sherman and FORD) respectively.

Details are aviilable in [`ae/README.md`](ae/README.md).

## Contact
For any questions, please contact us at `renfeng.chn AT outlook.com`.

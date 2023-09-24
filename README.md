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
1. Edit the configuration file `config/test_rdma.json`. A sample configuration file is shown below.
   ```json
   {
      "servers": [
         "optane06"     // the address of server side
      ],
      "port": 12345,
      "ib_socket": 1,
      "sockets": 2,     // number of CPU sockets
      "processors_per_socket": 48,  // number of cores per socket
      "block_size": 8,  // access granularity in bytes
      "qp_num": -1,     // -1 for per-thread QP/doorbell policies
      "dump_file_path": "test_rdma.csv",  // path of dump file
      "type": "read"    // can be `read`, `write` or `atomic`
   }
   ```
2. Change the current directory to the build sub-directory of the cloned repository (i.e., `cd /path/to/smart && cd build`) .


3. In server side (`optane06` in the sample), run the following command:
   ```bash
   LD_PRELOAD=libmlx5.so ./test/test_rdma
   ```
4. In client side (another machine), run the following command:
   ```bash
   LD_PRELOAD=libmlx5.so ./test/test_rdma \
      [nr_thread] \
      [outstanding_work_request_per_thread]
   ```
   For example,
   ```bash
   LD_PRELOAD=libmlx5.so ./test/test_rdma 96 8
   ```
5. After execution, the client displays the following to stdout. 
   ```
   rdma-read: #threads=96, #depth=8, #block\_size=8, BW=848.217 MB/s, IOPS=111.177 M/s, conn establish time=1245.924 ms
   ```
   It also adds a line to the file specified by `dump_file_path` (`test_rdma.csv` according to the sample conf).
   ```
   rdma-read, 96, 8, 8, 848.217, 111.177, 1245.924
   ```

## Functional Evaluation: Applications
For the sake of illustration, we present the functional evaluation method based on the following cluster. 
- **Memory Blades**: hostname `optane06` and `optane07`, with port `12345`,
- **Compute Blades**: hostname `optane04`

### Configuration Files

SMART and its applications rely on multiple configuration files. By default, they are located in the `config/` subdirectory. It is also possible to specify the configuration file to use through environment variables, including `SMART_CONFIG_PATH` and `APP_CONFIG_PATH`.

- `smart_config.json`: Options of the SMART framework, which are appliable to all kinds of servers. Set `use_thread_aware_alloc`, `use_work_req_throt`, `use_conflict_avoidance` and `use_speculative_lookup`  as `true` to enable thread-aware resource allocation, outstanding work request throttling, conflict avoidance and speculative lookup respectively.
  
- `backend.json`: Options of backend servers running in memory blades. The `dev_dax_path` field specifies whether DRAM or PMEM device is used. The `capacity` field determines the amount of memory to use.

- `datastructure.json`: Options of hashtable/B+tree clients running in the compute blades. Use `memory_servers` to specify **hostnames and ports** of memory blades to be connected. If you change the topology of the cluster, modify it accordingly.
  
- `transaction.json`: Options of transaction processing clients running in the compute blades. Use `memory_servers` to specify **hostnames and ports** of memory blades to be connected. If you change the topology of the cluster, modify it accordingly.

### Start Memory Blades
1. Check and update the configuration file `config/backend.json`. 
2. For each memory blade **(`optane06` and `optane07` in our example)**, run one of the following programs:
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
1. For hash table and B+Tree, you need to check `config/datastructure.json`. For distributed transactions, you need to check `config/transaction.json`. 
   - The `memory_servers` section contains target memory blades to be connected. 
     They must be launched in the previous step.
   - Both configuration files also include other parameters including workload types (e.g., `ycsb-a` to `ycsb-f`) and problem scales (see source code for detail).

2. In compute blades **(`optane04` in our example)**, run one of the benchmark program using the following command. It must match the backend started in the previous step. If `nr_thread` and `nr_coro` not specified, we will use the default value in the configuration file.
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
  After execution, the benchmark program displays the throughput and latency to stdout. It also adds a line to the file specified by `dump_file_path` (from `config/datastructure.json` or `config/transaction.json` respectively).


## Reproduce Evaluation
We also provide scripts to reproduce all experiments in Section 3 and Section 6. In Section 3, there are 3 experiments (Figures 3, 4 and 5), each of them points out one of the scalability bottlenecks. In Section 6, there are 9 experiments (Figures 7–14, and Table 1) that compare SMART-refactorized applications (i.e. SMART-HT, SMART-BT and SMART-DTX) with the state-of-the-art disaggregated systems (i.e. RACE, Sherman and FORD) respectively.

Details are aviilable in [`ae/README.md`](ae/README.md).
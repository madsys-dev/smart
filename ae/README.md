# Reproduce Evaluation
We also provide scripts to reproduce all experiments in Section 3 and Section 6. In Section 3, there are 3 experiments (Figures 3, 4 and 5), each of them points out one of the scalability bottlenecks. In Section 6, there are 9 experiments (Figures 7–14, and Table 1) that compare SMART-refactorized applications (i.e. SMART-HT, SMART-BT and SMART-DTX) with the state-of-the-art disaggregated systems (i.e. RACE, Sherman and FORD) respectively.

## Experiment workflow: Setting Clusters
It requires eight machines with SMART installed to to reproduce experimental results. 
**Passwordless
login must be enabled in this cluster.** 

In the scripts we provide, the test
cluster consists of machines whose hostnames are `optane00`, `optane01`, `optane02`, `optane03`,
`optane04`, `optane06`, `optane07`, and `optane08` and respectively. You can temporarily modify the hostnames of the individual
machines in the cluster during reproduce tests, or you can
replace hostnames appeared in the following configuration
files as appropriate:
### `ae/collect/common.sh`
```bash
# Update the project directory
export project_directory=/path/to/smart

# Hostnames of memory blades for micro-benchmark, hashtable, 
# B+-tree and distributed transaction system respectively
export rdma_test_backend=optane06
export hashtable_test_backends=optane06,optane07
export btree_test_backends=optane06,optane07,optane08,optane00,optane01,optane03,optane04,optane02
export dtx_test_backends=optane06,optane07

# Hostnames of compute blade set for hashtable and B+-tree
# `optane0[0,1,3]` is equivalent to `optane00,optane01,optane03`,
#   meaning three machines will be used
# fig7: 6 elements, the i-th element contains i machines.
#   avoid using machines in $hashtable_test_backends
export fig7_compute_machine_list=(optane00 optane0[0,1] optane0[0,1,3] optane0[0,1,3,4] optane0[0,1,3,4,2] optane0[0,1,3,4,2,8])

# fig12: 8 elements, the i-th element should contain the first i
#   hostnames of $btree_test_backends
export fig12_compute_machine_list=(optane06 optane0[6,7] optane0[6,7,8] optane0[6,7,8,0] optane0[6,7,8,0,1] optane0[6,7,8,0,1,3] optane0[6,7,8,0,1,3,4] optane0[6,7,8,0,1,3,4,2])

# You should run collect scripts in the following machine
#   avoid using machines in $rdma_test_backend, 
#   $hashtable_test_backends or $dtx_test_backends
export client_machine=optane04
```
### `config/test_rdma.json`
```json
{
   // keep consistent with $rdma_test_backend
   "servers": [ "optane06" ],
}
```
### `config/transaction.json`
   ```json
   {
      // keep consistent with $dtx_test_backends
      "memory_servers": [
         { "hostname": "optane06", "port": 12345 },
         { "hostname": "optane07", "port": 12345 }
      ]
   }
   ```
### `ae/collect/config/datastructure.hashtable.json`
```json
{
   // keep consistent with $hashtable_test_backends
   "memory_servers": [
      { "hostname": "optane06", "port": 12345 },
      { "hostname": "optane07", "port": 12345 }
   ]
}
```
### `ae/collect/config/datastructure.btree.json`
```json
{
   // keep consistent with $btree_test_backends, including the order
   "memory_servers": [
      { "hostname": "optane06", "port": 12345 },
      { "hostname": "optane07", "port": 12345 },
      { "hostname": "optane08", "port": 12345 },
      // ...
      { "hostname": "optane02", "port": 12345 },
}
```
### `ae/plot/common.py`
```python
# Update the project directory
project_directory = '/path/to/smart'
```

## Experiment workflow: Execute
On the `$client_machine` machine (i.e., `optane04` in our configuration file), execute `run.sh` in artifact's
root directory, which runs all the experiments and generate the
figures. 

If only part of the test needs to be performed, you can comment out unnecessary lines in both `ae/collect/runall.sh` and `ae/plot/runall.sh`.

## Evaluation and expected results
The test scripts produce numerical results in the `ae/raw/*.csv`, and figures in `ae/figure/*.pdf`. We provide reference results in `ae/raw-reference/*.csv` and `ae/figure-reference/*.pdf` respectively.

For Table 1, the result is available in `ae/raw/table1.csv`. 

## Reproducing Figure 4b
Figure 4b requires the use of **Mellanox Neo-Host** to collect performance metrics, which is proprietary software that can be obtained by contacting your NVIDIA reseller.

To complete the reproduction of Figure 4b, the following steps are needed:
1. Launch two terminal windows (Terminal A and B), each connected to `optane04`;
2. in Terminal A, execute `sudo mst start` and locate the MST device corresponding to the RNIC (e.g. `/dev/mst/mt4123_pciconf0`).
3. In Terminal B, execute `bash ae/collect/fig4b-owr-pcie-perf.sh`.
4. For each thread/depth combination, execute the following command in Terminal A:
   > sudo python3 /opt/neohost/sdk/get_device_performance_counters.py --dev-mst /dev/mst/mt4123_pciconf0 | grep 'PCIe Inbound Used BW'

   This prints the current PCIe Inbound bandwidth (Gb/s). You can calculate the average DRAM access traffic per work request (bytes) by dividing the PCIe Inbound bandwidth by IOPS.

## Experiment customization
The parallelism for the real-world applications performance evaluation experiment can be modified by setting the `thread_set` and `depth_set` variables to appropriate number of threads and coroutines in the scripts. You should modify `ae/collect/common.sh` and `ae/plot/common.py` together.

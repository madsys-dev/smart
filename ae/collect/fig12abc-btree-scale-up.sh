#!/bin/bash
source ../ae/collect/common.sh
export MEMORY_NODES=1
export DEPTH=8
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.hashtable.json
export USE_HOCL=1

function start_backend() {
  bash ../ae/collect/backends/run_btree_backend_local.sh &
  sleep 10
  export MAX_THREADS=$max_threads
  export MY_DEPTH=1
  # Loading dataset
  APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.local.json \
  INSERT_ONLY=1 \
  bash ../ae/collect/test_app/btree.sh ${MAX_THREADS} ${MY_DEPTH}
}

function kill_backend() {
  killall -q run_btree_backend_local.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run_thread_count() {
  start_backend
  
  # Sherman
  for threads in ${fig12_thread_set[@]}; do
    # Dump one record per execution 
    # The format is exactly the same as hashtable benchmarks
    DUMP_FILE_PATH=../ae/raw/fig12-${WORKLOAD}.csv \
    SMART_OPTS=None \
    DATASET_PATH=${WORKLOAD} \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.local.json \
    bash ../ae/collect/test_app/btree.sh ${threads} ${DEPTH}
  done

  # Sherman with SpecLookup
  for threads in ${fig12_thread_set[@]}; do
    DUMP_FILE_PATH=../ae/raw/fig12-${WORKLOAD}.csv \
    SMART_OPTS=SpecLookup \
    DATASET_PATH=${WORKLOAD} \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.local.json \
    bash ../ae/collect/test_app/btree.sh ${threads} ${DEPTH}
  done

  # SMART-BT
  for threads in ${fig12_thread_set[@]}; do
    DUMP_FILE_PATH=../ae/raw/fig12-${WORKLOAD}.csv \
    DATASET_PATH=${WORKLOAD} \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid,SpecLookup \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.local.json \
    bash ../ae/collect/test_app/btree.sh ${threads} ${DEPTH}
  done

  kill_backend
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
export WORKLOAD=ycsb-a
run_thread_count
export WORKLOAD=ycsb-b
run_thread_count
export WORKLOAD=ycsb-c
run_thread_count
exit 0

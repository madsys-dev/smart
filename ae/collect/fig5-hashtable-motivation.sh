#!/bin/bash
source ../ae/collect/common.sh

export MEMORY_NODES=2
export DEPTH=8
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.hashtable.json

function start_backend() {
  bash ../ae/collect/backends/run_hashtable_backend.sh &
  sleep 10
  export MAX_THREADS=$max_threads
  export MY_DEPTH=1
  # Loading dataset
  APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
  INSERT_ONLY=1 \
  bash ../ae/collect/test_app/hashtable.sh ${MAX_THREADS} ${MY_DEPTH}
}

function kill_backend() {
  killall -q run_hashtable_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run_thread_count() {
  for threads in ${fig5_thread_set[@]}; do
    start_backend
    
    # The following command produces one line output of fig5{a,b}.csv.
    # Output format:
    # HashTableMultiShard, update-only,  1,       8,     8,       8,         100000000, 246280,            30342,              67250
    # app_name,            dataset-name, threads, depth, key_len, value_len, num_keys,  throuhgput (OP/s), median latency(ns), tail latency(ns)
    # Note that if REPORT_LATENCY is NOT enabled, median latency and tail latency are marked as '-1'

    DUMP_FILE_PATH=../ae/raw/fig5a.csv \
    SMART_OPTS=None \
    DATASET_PATH=update-only \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    bash ../ae/collect/test_app/hashtable.sh ${threads} ${DEPTH}

    kill_backend
  done
}

function run_zipfian() {
  export THREADS=16
  for zipfian in ${fig5_zipfian_set[@]}; do
    start_backend

    # Dump one record per execution 
    DUMP_FILE_PATH=../ae/raw/fig5b.csv \
    SMART_OPTS=None \
    DATASET_PATH=update-only \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    ZIPFIAN_CONST=${zipfian} \
    REPORT_LATENCY=1 \
    bash ../ae/collect/test_app/hashtable.sh ${THREADS} ${DEPTH}

    kill_backend
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
run_thread_count
run_zipfian
exit 0

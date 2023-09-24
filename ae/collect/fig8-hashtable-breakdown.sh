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
  # RACE
  for threads in ${thread_set[@]}; do
    start_backend

    DUMP_FILE_PATH=../ae/raw/fig8-${WORKLOAD}.csv \
    SMART_OPTS=None \
    DATASET_PATH=${WORKLOAD} \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    bash ../ae/collect/test_app/hashtable.sh ${threads} ${DEPTH}

    kill_backend
  done

  # +ThrdAwareAlloc
  for threads in ${thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig8-${WORKLOAD}.csv \
    SMART_OPTS=ThrdAwareAlloc \
    DATASET_PATH=${WORKLOAD} \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    bash ../ae/collect/test_app/hashtable.sh ${threads} ${DEPTH}

    kill_backend
  done

  # +WorkReqThrot
  for threads in ${thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig8-${WORKLOAD}.csv \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot \
    DATASET_PATH=${WORKLOAD} \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    bash ../ae/collect/test_app/hashtable.sh ${threads} ${DEPTH}

    kill_backend
  done

  # SMART-HT
  for threads in ${thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig8-${WORKLOAD}.csv \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    DATASET_PATH=${WORKLOAD} \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    bash ../ae/collect/test_app/hashtable.sh ${threads} ${DEPTH}

    kill_backend
  done
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

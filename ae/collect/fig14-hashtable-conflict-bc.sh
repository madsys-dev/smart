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
  bash ../ae/collect/test_app/hashtable-prof.sh ${MAX_THREADS} ${MY_DEPTH}
}

function kill_backend() {
  killall -q run_hashtable_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run_thread_count() {
  # w/o ConflictAvoid
  for threads in ${fig14_thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig14bc.csv \
    DATASET_PATH=update-only \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    bash ../ae/collect/test_app/hashtable-prof.sh ${threads} ${DEPTH}

    kill_backend
  done

  # +Backoff
  for threads in ${fig14_thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig14bc.csv \
    DATASET_PATH=update-only \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    DISABLE_CORO_THROT=1 MAX_BACKOFF_CYCLE_LOG2=-1 \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    bash ../ae/collect/test_app/hashtable-prof.sh ${threads} ${DEPTH}

    kill_backend
  done

  # +DynBackoffLimit
  for threads in ${fig14_thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig14bc.csv \
    DATASET_PATH=update-only \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    DISABLE_CORO_THROT=1 \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    bash ../ae/collect/test_app/hashtable-prof.sh ${threads} ${DEPTH}

    kill_backend
  done

  # +CoroThrot
  for threads in ${fig14_thread_set[@]}; do
    start_backend
    
    DUMP_FILE_PATH=../ae/raw/fig14bc.csv \
    DATASET_PATH=update-only \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    bash ../ae/collect/test_app/hashtable-prof.sh ${threads} ${DEPTH}

    kill_backend
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
run_thread_count
exit 0

# NOTE:
# To collect retries count per operation (Fig 14b and 14c),
# rebuild this project with #define CONFIG_STAT, then run this script again.

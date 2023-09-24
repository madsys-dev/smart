#!/bin/bash
source ../ae/collect/common.sh

export MEMORY_NODES=2
export DEPTH=8
export THREADS=$max_threads
export idle_set=(0 16 32 64 128 256 512 1024 2048 4096 8192)
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.hashtable.json

function start_backend() {
  bash ../ae/collect/backends/run_hashtable_backend.sh &
  sleep 10
  export MY_DEPTH=1
  # Loading dataset
  APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
  INSERT_ONLY=1 \
  bash ../ae/collect/test_app/hashtable.sh ${THREADS} ${MY_DEPTH}
}

function kill_backend() {
  killall -q run_hashtable_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run() {
  # RACE
  for idle in ${idle_set[@]}
  do
    start_backend

    DUMP_FILE_PATH=../ae/raw/fig9.csv \
    SMART_OPTS=None \
    DATASET_PATH=ycsb-c \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    IDLE_USEC=${idle} \
    bash ../ae/collect/test_app/hashtable.sh ${THREADS} ${DEPTH}
    
    kill_backend
  done

  # SMART-BT
  for idle in ${idle_set[@]}
  do
    start_backend

    DUMP_FILE_PATH=../ae/raw/fig9.csv \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    DATASET_PATH=ycsb-c \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    REPORT_LATENCY=1 \
    IDLE_USEC=${idle} \
    bash ../ae/collect/test_app/hashtable.sh ${THREADS} ${DEPTH}
    
    kill_backend
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
run
exit 0

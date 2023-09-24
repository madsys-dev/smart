#!/bin/bash
source ../ae/collect/common.sh

export MEMORY_NODES=2
export DEPTH=8
export THRAEDS=$max_threads
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.hashtable.json

function start_backend() {
  bash ../ae/collect/backends/run_hashtable_backend.sh &
  sleep 10
  export MAX_THREADS=96
  export MY_DEPTH=1
  # Loading dataset
  APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
  INSERT_ONLY=1 \
  bash ../ae/collect/test_app/hashtable.sh ${THRAEDS} ${MY_DEPTH}
}

function kill_backend() {
  killall -q run_hashtable_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run_thread_count() {
  # RACE
  declare -i idx=1
  for machine in ${fig7_compute_machine_list[@]}; do
    start_backend

    clush -w $machine \
    bash `pwd`/../ae/collect/test_app/hashtable-remote.sh \
         `pwd` $WORKLOAD \
         None $idx ${fig7_compute_machine_list[0]}

    let idx++
    kill_backend
  done

  # SMART-HT
  declare -i idx=1
  for machine in ${fig7_compute_machine_list[@]}; do
    start_backend

    clush -w $machine \
    bash `pwd`/../ae/collect/test_app/hashtable-remote.sh \
         `pwd` $WORKLOAD \
         ThrdAwareAlloc,WorkReqThrot,ConflictAvoid $idx ${fig7_compute_machine_list[0]}

    let idx++
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

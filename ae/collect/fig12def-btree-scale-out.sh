#!/bin/bash

source ../ae/collect/common.sh
export MEMORY_NODES=2
export DEPTH=8
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.hashtable.json
export USE_HOCL=1

function start_backend() {
  bash ../ae/collect/backends/run_btree_backend.sh &
  sleep 30
  export MY_DEPTH=1
  export MEMORY_NODES=$1
  # Loading dataset
  APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.json \
  INSERT_ONLY=1 \
  bash ../ae/collect/test_app/btree.sh ${fig12_max_threads} ${MY_DEPTH}
  sleep 5
}

function kill_backend() {
  sleep 5
  killall -q run_btree_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
  sleep 5
}

function run_thread_count() {
  # Sherman
  declare -i idx=1
  for machine in ${fig12_compute_machine_list[@]}; do
    start_backend $idx

    clush -w $machine \
    bash `pwd`/../ae/collect/test_app/btree-remote.sh \
         `pwd` $WORKLOAD \
         None $idx ${fig12_compute_machine_list[0]}

    let idx++
    kill_backend
  done

  # Sherman with SpecLookup
  declare -i idx=1
  for machine in ${fig12_compute_machine_list[@]}; do
    start_backend $idx

    clush -w $machine \
    bash `pwd`/../ae/collect/test_app/btree-remote.sh \
         `pwd` $WORKLOAD \
         SpecLookup $idx ${fig12_compute_machine_list[0]}

    let idx++
    kill_backend
  done

  # SMART-BT
  declare -i idx=1
  for machine in ${fig12_compute_machine_list[@]}; do
    start_backend $idx

    clush -w $machine \
    bash `pwd`/../ae/collect/test_app/btree-remote.sh \
         `pwd` $WORKLOAD \
         ThrdAwareAlloc,WorkReqThrot,ConflictAvoid,SpecLookup $idx \
         ${fig12_compute_machine_list[0]}

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

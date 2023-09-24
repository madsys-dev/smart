#!/bin/bash
export BLKSIZE=8

source ../ae/collect/common.sh
function start_backend() {
  bash ../ae/collect/backends/run_rdma_backend.sh &
  sleep 10
}

function kill_backend() {
  killall -q run_rdma_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

# see fig3-scalability.sh for output format
function run_thread_count() { 
  export DUMP_FILE_PATH=../ae/raw/fig13a.csv
  export thread_set=(1 8 16 24 32 40 48 56 64 72 80 88 96)
  export DEPTH=16
  for threads in ${thread_set[@]}; do
    bash ../ae/collect/test_rdma/per_thread_qp.sh ${threads} ${DEPTH}
  done
  for threads in ${thread_set[@]}; do
    # +ThrdResAlloc
    bash ../ae/collect/test_rdma/per_thread_doorbell.sh ${threads} ${DEPTH}
  done
  for threads in ${thread_set[@]}; do
    # +WorkReqThrot
    bash ../ae/collect/test_rdma/with_work_req_throt.sh ${threads} ${DEPTH}
  done
}

function run_batch_size() {
  export DUMP_FILE_PATH=../ae/raw/fig13b.csv
  export THREAD=$max_threads
  export depth_set=(1 4 8 12 16 20 24 28 32)
  for depth in ${depth_set[@]}; do
    bash ../ae/collect/test_rdma/per_thread_qp.sh ${THREAD} ${depth}
  done
  for depth in ${depth_set[@]}; do
    # +ThrdResAlloc
    bash ../ae/collect/test_rdma/per_thread_doorbell.sh ${THREAD} ${depth}
  done
  for depth in ${depth_set[@]}; do
    # +WorkReqThrot
    bash ../ae/collect/test_rdma/with_work_req_throt.sh ${THREAD} ${depth}
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
kill_backend
start_backend
export TYPE=read
run_thread_count
run_batch_size
kill_backend
exit 0

#!/bin/bash
source ../ae/collect/common.sh

export BLKSIZE=8
export DEPTH=8

function start_backend() {
  bash ../ae/collect/backends/run_rdma_backend.sh &
  sleep 10
}

function kill_backend() {
  killall -q run_rdma_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run() {
  # Lines 1-13 of fig3{a,b}.csv, each line containing the following information
  # e.g. #1: test_rdma, 1,       8,     8,       14.186,            1.859
  #          program,   threads, depth, blksize, throughput (MB/s), IOPS (MOP/s)
  for threads in ${thread_set[@]}; do
    bash ../ae/collect/test_rdma/shared_qp.sh ${threads} ${DEPTH}
  done
  # Lines 14-26 of fig3{a,b}.csv
  for threads in ${thread_set[@]}; do
    bash ../ae/collect/test_rdma/multiplexed_qp_4.sh ${threads} ${DEPTH}
  done
  # Lines 27-39 of fig3{a,b}.csv
  for threads in ${thread_set[@]}; do
    bash ../ae/collect/test_rdma/multiplexed_qp_2.sh ${threads} ${DEPTH}
  done
  # Lines 40-52 of fig3{a,b}.csv
  for threads in ${thread_set[@]}; do
    bash ../ae/collect/test_rdma/per_thread_qp.sh ${threads} ${DEPTH}
  done
  # Lines 53-65 of fig3{a,b}.csv
  for threads in ${thread_set[@]}; do
    bash ../ae/collect/test_rdma/per_thread_doorbell.sh ${threads} ${DEPTH}
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
kill_backend
start_backend
export DUMP_FILE_PATH=../ae/raw/fig3a.csv
export TYPE=read
run
export DUMP_FILE_PATH=../ae/raw/fig3b.csv
export TYPE=write
run
kill_backend
exit 0

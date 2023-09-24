#!/bin/bash
source ../ae/collect/common.sh

export BLKSIZE=8

function start_backend() {
  bash ../ae/collect/backends/run_rdma_backend.sh &
  sleep 10
}

function kill_backend() {
  killall -q run_rdma_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run() {
  for threads in ${fig4_thread_set[@]}; do
    for depth in ${fig4_depth_set[@]}; do
      echo "Running: type=$TYPE threads=$threads depth=$depth"
      # Please manually profile PCIe inbound traffic using Mellanox Neohost
      bash ../ae/collect/test_rdma/per_thread_doorbell.sh ${threads} ${depth}
      read -p "Press any key to resume ..."
    done
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
kill_backend
start_backend
export DUMP_FILE_PATH=../ae/raw/fig4-prof.csv
export TYPE=write
run
kill_backend
exit 0

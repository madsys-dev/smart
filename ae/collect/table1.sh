#!/bin/bash
source ../ae/collect/common.sh

export BLKSIZE=8
export DEPTH=64

# export sample_cycles=19200000
# export execution_epochs=60

function start_backend() {
  bash ../ae/collect/backends/run_rdma_backend.sh &
  sleep 10
}

function kill_backend() {
  killall -q run_rdma_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

function run() {
  for interval in ${interval_set[@]}; do
    LD_PRELOAD=libmlx5.so \
    SMART_OPTS=ThrdAwareAlloc \
    DUMP_PREFIX='rdma-'${TYPE}'-per-doorbell-'${interval} \
    numactl --interleave=all ./test/test_rdma_dyn ${max_threads} ${DEPTH} ${interval}
  done
  for interval in ${interval_set[@]}; do
    LD_PRELOAD=libmlx5.so \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot \
    DUMP_PREFIX='rdma-'${TYPE}'-work-req-throt-'${interval} \
    numactl --interleave=all ./test/test_rdma_dyn ${max_threads} ${DEPTH} ${interval}
  done
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
kill_backend
start_backend
export DUMP_FILE_PATH=../ae/raw/table1.csv
export TYPE=read
run
kill_backend
exit 0

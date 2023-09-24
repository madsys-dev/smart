#!/bin/bash

source ../ae/collect/common.sh
export THREADS=$max_threads
export DEPTH=8
export APP=smallbank
export idle_set=(0 16 32 64 128 256 512 1024 2048 4096 8192)
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.dtx.json

function run_app() {
  bash ../ae/collect/backends/run_${APP}_backend.sh &
  sleep 10
  
  # FORD
  export DUMP_PREFIX='ford-'${APP}
  for idle in ${idle_set[@]}; do
    DUMP_FILE_PATH=../ae/raw/fig11-${APP}.csv \
    SMART_OPTS=None \
    IDLE_USEC=${idle} \
    bash ../ae/collect/test_app/${APP}.sh ${THREADS} ${DEPTH}
  done

  # SMART-DTX
  export DUMP_PREFIX='dtx-'${APP}
  for idle in ${idle_set[@]}; do
    DUMP_FILE_PATH=../ae/raw/fig11-${APP}.csv \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    IDLE_USEC=${idle} \
    bash ../ae/collect/test_app/${APP}.sh ${THREADS} ${DEPTH}
  done

  killall -q run_${APP}_backend.sh
  bash ../ae/collect/backends/kill_backend.sh
}

if [ ! -f test/test_rdma ]; then
  echo 'Please run this script under the `build` directory'
  exit 127
fi
export APP=smallbank
run_app
export APP=tatp
run_app
exit 0

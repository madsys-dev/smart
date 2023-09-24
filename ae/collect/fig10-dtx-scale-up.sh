#!/bin/bash
source ../ae/collect/common.sh

export DEPTH=8
export APP=smallbank
export SMART_CONFIG_PATH=../ae/collect/config/smart_config.dtx.json

function run_app() {
  bash ../ae/collect/backends/run_${APP}_backend.sh &
  sleep 10
  
  # FORD
  export DUMP_PREFIX='ford-'${APP}
  for threads in ${thread_set[@]}; do
    # The following command performs one bench and append the result to the dump file. Dump format: 
    # dtx-smallbank, 1,       8,     0.244, 0.21,                           35.272,              62.335, 0.141, 13.106, 3.2
    # name,          threads, depth,      , committed txns thrput (Mtxn/s), median latency (us), tail latency (us), other information
    DUMP_FILE_PATH=../ae/raw/fig10-${APP}.csv \
    SMART_OPTS=None \
    bash ../ae/collect/test_app/${APP}.sh ${threads} ${DEPTH}
  done

  # SMART-DTX
  export DUMP_PREFIX='dtx-'${APP}
  for threads in ${thread_set[@]}; do
    DUMP_FILE_PATH=../ae/raw/fig10-${APP}.csv \
    SMART_OPTS=ThrdAwareAlloc,WorkReqThrot,ConflictAvoid \
    bash ../ae/collect/test_app/${APP}.sh ${threads} ${DEPTH}
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

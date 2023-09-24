#!/bin/bash
cd $1

source ../ae/collect/common.sh

export MEMORY_NODES=2
export COMPUTE_NODES=$4
export DEPTH=8
export THRAEDS=${max_threads}

if [[ $5 == `hostname` ]]; then
    DUMP_FILE_PATH=../ae/raw/fig7-$2.csv \
    DATASET_PATH=$2 \
    SMART_OPTS=$3 \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    LD_PRELOAD=libmlx5.so numactl --interleave=all ./hashtable/hashtable_bench $THRAEDS $DEPTH
else
    DATASET_PATH=$2 \
    SMART_OPTS=$3 \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.hashtable.json \
    LD_PRELOAD=libmlx5.so numactl --interleave=all ./hashtable/hashtable_bench $THRAEDS $DEPTH
fi
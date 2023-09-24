#!/bin/bash
cd $1

source ../ae/collect/common.sh

export MEMORY_NODES=$4
export COMPUTE_NODES=$4
export DEPTH=8
export THRAEDS=${fig12_max_threads}
export USE_HOCL=1

if [[ $5 == `hostname` ]]; then
    DUMP_FILE_PATH=../ae/raw/fig12a-$2.csv \
    DATASET_PATH=$2 \
    SMART_OPTS=$3 \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.json \
    LD_PRELOAD=libmlx5.so numactl --interleave=all ./btree/btree_bench $THRAEDS $DEPTH
else
    DATASET_PATH=$2 \
    SMART_OPTS=$3 \
    APP_CONFIG_PATH=../ae/collect/config/datastructure.btree.json \
    LD_PRELOAD=libmlx5.so numactl --interleave=all ./btree/btree_bench $THRAEDS $DEPTH
fi
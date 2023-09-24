#!/bin/bash
LD_PRELOAD=libmlx5.so \
SMART_CONFIG_PATH=../ae/collect/config/smart_config.shared_qp.json \
QP_NUM='-2' \
DUMP_PREFIX='rdma-'${TYPE}'-mul-2' \
numactl --interleave=all ./test/test_rdma $1 $2

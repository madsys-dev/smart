#!/bin/bash
LD_PRELOAD=libmlx5.so \
SMART_OPTS=ThrdAwareAlloc \
DUMP_PREFIX='rdma-'${TYPE}'-per-doorbell' \
numactl --interleave=all ./test/test_rdma $1 $2

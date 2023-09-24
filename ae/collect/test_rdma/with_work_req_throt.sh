#!/bin/bash
LD_PRELOAD=libmlx5.so \
SMART_OPTS=ThrdAwareAlloc,WorkReqThrot \
DUMP_PREFIX='rdma-'${TYPE}'-work-req-throt' \
numactl --interleave=all ./test/test_rdma $1 $2

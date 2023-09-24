#!/bin/bash
LD_PRELOAD=libmlx5.so \
SMART_OPTS=None \
DUMP_PREFIX='rdma-'${TYPE}'-per-qp' \
numactl --interleave=all ./test/test_rdma $1 $2

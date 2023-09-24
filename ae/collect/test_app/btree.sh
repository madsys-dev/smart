#!/bin/bash
LD_PRELOAD=libmlx5.so numactl --interleave=all ./btree/btree_bench $1 $2
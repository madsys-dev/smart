#!/bin/bash
LD_PRELOAD=libmlx5.so numactl --interleave=all ./hashtable/hashtable_bench_prof $1 $2
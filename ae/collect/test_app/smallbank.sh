#!/bin/bash
LD_PRELOAD=libmlx5.so numactl --interleave=all ./dtx/smallbank/smallbank_bench $1 $2
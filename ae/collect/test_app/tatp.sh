#!/bin/bash
LD_PRELOAD=libmlx5.so numactl --interleave=all ./dtx/tatp/tatp_bench $1 $2
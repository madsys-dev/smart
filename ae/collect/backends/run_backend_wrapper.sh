#!/bin/bash
cd $2
LD_PRELOAD=libmlx5.so numactl -m1 -N1 $1

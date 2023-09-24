#!/bin/bash
mkdir build
cd build
cmake ..
make -j
cp ../patch/libmlx5.so libmlx5.so
echo 'Build completed'
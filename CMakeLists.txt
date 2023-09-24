cmake_minimum_required(VERSION 3.16)
project(sds CXX ASM)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3 -g")

include_directories(include)

add_subdirectory(util)
add_subdirectory(smart)

add_subdirectory(test)

add_subdirectory(hashtable)
add_subdirectory(btree)
add_subdirectory(dtx)

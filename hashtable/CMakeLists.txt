add_executable(hashtable_backend backend.cpp)
target_link_libraries(hashtable_backend PUBLIC smart)

add_executable(hashtable_bench bench.cpp hashtable.cpp)
target_link_libraries(hashtable_bench PUBLIC smart)

add_executable(hashtable_bench_prof bench.cpp hashtable.cpp)
target_compile_definitions(hashtable_bench_prof PUBLIC -DCONFIG_STAT)
target_link_libraries(hashtable_bench_prof PUBLIC smart)
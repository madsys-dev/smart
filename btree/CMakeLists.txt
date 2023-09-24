add_executable(btree_backend backend.cpp btree.cpp)
target_link_libraries(btree_backend PUBLIC smart)

add_executable(btree_bench bench.cpp btree.cpp)
target_link_libraries(btree_bench PUBLIC smart)

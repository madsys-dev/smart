add_executable(ycsb_gen ycsb_gen.cpp)
target_link_libraries(ycsb_gen PUBLIC pthread smart)

add_subdirectory(fcontext)
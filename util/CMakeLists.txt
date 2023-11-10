add_executable(ycsb_gen ycsb_gen.cpp)
target_link_libraries(ycsb_gen PUBLIC pthread smart)

if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "aarch64")
    add_subdirectory(fcontext-arm64)
else()
    add_subdirectory(fcontext-x64)
endif()

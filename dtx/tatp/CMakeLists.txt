add_executable(tatp_backend backend.cpp)
target_link_libraries(tatp_backend PUBLIC dtx_tatp)

add_executable(tatp_bench bench.cpp)
target_link_libraries(tatp_bench PUBLIC dtx_tatp)

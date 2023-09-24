add_library(dtx_smallbank dtx.cpp)
target_compile_definitions(dtx_smallbank PUBLIC CONFIG_ITEM_SIZE=8)
target_link_libraries(dtx_smallbank PUBLIC smart)

add_library(dtx_tatp dtx.cpp)
target_compile_definitions(dtx_tatp PUBLIC CONFIG_ITEM_SIZE=40)
target_link_libraries(dtx_tatp PUBLIC smart)

add_subdirectory(smallbank)
add_subdirectory(tatp)

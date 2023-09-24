add_library(smart initiator.cpp target.cpp resource_manager.cpp task.cpp thread.cpp)
target_link_libraries(smart PUBLIC numa pthread fcontext ibverbs mlx5)

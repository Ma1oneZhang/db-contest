set(SOURCES sm_manager.cpp)
add_library(system STATIC ${SOURCES})
target_link_libraries(system index record)

add_executable(sm_manager_test sm_manager_test.cpp)
target_link_libraries(sm_manager_test system gtest_main)
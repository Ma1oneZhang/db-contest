set(SOURCES rm_file_handle.cpp rm_scan.cpp)
add_library(record STATIC ${SOURCES})
add_library(records SHARED ${SOURCES})
target_link_libraries(record system transaction system storage)

add_executable(rm_test rm_test.cpp)
target_link_libraries(rm_test record gtest_main)

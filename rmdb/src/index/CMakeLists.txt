set(SOURCES ix_index_handle.cpp ix_scan.cpp)
add_library(index STATIC ${SOURCES})
target_link_libraries(index storage)

add_executable(index_insert_test_fake b_plus_tree_insert_test.cpp)
target_link_libraries(index_insert_test_fake gtest_main index)

add_executable(index_delete_test_fake b_plus_tree_delete_test.cpp)
target_link_libraries(index_delete_test_fake gtest_main index)
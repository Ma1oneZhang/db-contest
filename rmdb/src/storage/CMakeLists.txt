set(SOURCES 
        disk_manager.cpp 
        buffer_pool_manager.cpp 
        ../replacer/replacer.h 
        ../replacer/lru_replacer.cpp 
)
add_library(storage STATIC ${SOURCES})

#add_executable(storage_test disk_manager_test.cpp)
#target_link_libraries(storage_test storage gtest_main)
find_package(GTest CONFIG REQUIRED)

add_executable(storage_test
    storage_test.cpp
)

target_link_libraries(storage_test
    PRIVATE
    storage
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(storage_test)

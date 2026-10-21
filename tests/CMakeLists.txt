find_package(GTest CONFIG REQUIRED)

add_executable(storage_test
    storage/endian_test.cpp
    storage/record_writer_test.cpp
    storage/record_reader_test.cpp
    storage/atomic_file_test.cpp
    storage/staged_record_writer_test.cpp
)

target_link_libraries(storage_test
    PRIVATE
    storage
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(storage_test)

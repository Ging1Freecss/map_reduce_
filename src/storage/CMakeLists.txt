add_library(storage
    record_writer.cpp
    record_reader.cpp
    atomic_file.cpp
    endian.cpp
    staged_record_writer.cpp
)
target_include_directories(storage PUBLIC ${CMAKE_SOURCE_DIR}/include)

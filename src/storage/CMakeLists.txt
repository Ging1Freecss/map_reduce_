add_library(storage
    record_writer.cpp
    record_reader.cpp
    atomic_file.cpp
    endian.cpp
)
target_include_directories(storage PUBLIC ${CMAKE_SOURCE_DIR}/include)

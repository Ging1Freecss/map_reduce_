add_library(engine
    vector_value_iterator.cpp
    map_emit_context.cpp
    reduce_emit_context.cpp
)

target_include_directories(engine PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(engine PUBLIC storage)

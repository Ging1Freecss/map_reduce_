add_library(proto_lib STATIC)

asio_grpc_protobuf_generate(
    TARGET proto_lib
    GENERATE_GRPC
    USAGE_REQUIREMENT PUBLIC
    IMPORT_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}/vcpkg_installed/x64-linux/include
    /home/hp/vcpkg/installed/x64-linux/include
    PROTOS
    helloworld.proto
    mapreduce.proto
    shuffle.proto
    mr_coordination.proto
)

target_link_libraries(proto_lib
    PUBLIC
    gRPC::grpc++
    protobuf::libprotobuf
)

target_include_directories(proto_lib
    PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

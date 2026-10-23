cmake_minimum_required(VERSION 3.22)
project(map_reduce LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(asio-grpc CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS asio)

include_directories(${CMAKE_SOURCE_DIR}/include)

add_subdirectory(proto)
add_subdirectory(server)
add_subdirectory(client)
add_subdirectory(src/storage)


enable_testing()
add_subdirectory(tests)

# Automatically link compile_commands.json to the root folder on build for LSP
if(CMAKE_EXPORT_COMPILE_COMMANDS)
    add_custom_target(lsp_symlink ALL
        COMMAND ${CMAKE_COMMAND} -E create_symlink
        "${CMAKE_BINARY_DIR}/compile_commands.json"
        "${CMAKE_SOURCE_DIR}/compile_commands.json"
        COMMENT "Symlinking compile_commands.json to workspace root for LSP"
    )
endif()

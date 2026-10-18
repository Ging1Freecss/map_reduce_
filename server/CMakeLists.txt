add_executable(server
    main.cpp
    server.cpp
    server.hpp
)

target_link_libraries(server
    PRIVATE
    proto_lib
    asio-grpc::asio-grpc
    Boost::asio
)

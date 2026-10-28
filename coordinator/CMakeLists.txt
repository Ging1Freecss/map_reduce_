add_executable(coordinator
    main.cpp
    server.cpp
    server.hpp
    state.hpp
    state.cpp
)

target_link_libraries(coordinator
    PRIVATE
    proto_lib
    asio-grpc::asio-grpc
    Boost::asio
)

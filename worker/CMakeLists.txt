add_executable(worker
    main.cpp
    server.cpp
    server.hpp
    state.hpp
    state.cpp
)

target_link_libraries(worker
    PRIVATE
    proto_lib
    storage
    asio-grpc::asio-grpc
    Boost::asio
)


cmake_minimum_required(VERSION 3.15)
project(eau2)

set(CMAKE_CXX_STANDARD 14)

#add_executable(server server.cpp)
#add_executable(client client.cpp)
add_executable(main test/build.cpp)
add_executable(test test/test.cpp)


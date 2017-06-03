cmake_minimum_required(VERSION 3.6)
project(osTargil3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(SOURCE_FILES MapReduceSearch.h MapReduceSearch.cpp MapReduceClientUser.h
        MapReduceClientUser.cpp MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h
        Search.cpp)

add_executable(osTargil3 ${SOURCE_FILES})
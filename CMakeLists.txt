cmake_minimum_required(VERSION 3.6)
project(osTargil3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES check.cpp MapReduceSearch.h MapReduceSearch.cpp MapReduceClient.h
        MapReduceClientUser.cpp MapReduceClientUser.h)

add_executable(osTargil3 ${SOURCE_FILES})


cmake_minimum_required(VERSION 3.9)
project(librdkafka-example)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread -lrdkafka++")
include_directories("/usr/local/include")

add_executable(Producer KafkaProducer.cpp)
add_executable(Consumer KafkaConsumer.cpp)
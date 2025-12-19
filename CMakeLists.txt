cmake_minimum_required(VERSION 3.10)
project(MessageQueue)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -pedantic -std=c++17)
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Create executable
add_executable(message_queue_demo 
    main.cpp
    commitlog.hpp
    partition.hpp
    consumer_group.hpp
    broker.hpp
    producer.hpp
)

# Link libraries
target_link_libraries(message_queue_demo PRIVATE pthread)

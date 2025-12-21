cmake_minimum_required(VERSION 3.10)
project(hyperQ VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -Wall -DNDEBUG")

find_package(Threads REQUIRED)
include_directories(include)

add_subdirectory(src)
add_subdirectory(apps)
enable_testing()
add_subdirectory(tests)

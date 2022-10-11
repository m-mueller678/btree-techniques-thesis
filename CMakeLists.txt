cmake_minimum_required(VERSION 3.23)
project(btree)

set(CMAKE_CXX_STANDARD 20)

add_executable(btree test.cpp btree.cpp btree.h PerfEvent.hpp)

target_compile_options(${PROJECT_NAME} PRIVATE
        -Wall -Wextra -Werror
        )
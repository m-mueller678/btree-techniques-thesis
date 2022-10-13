cmake_minimum_required(VERSION 3.23)
project(btree)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 20)

add_executable(btree test.cpp btree.h PerfEvent.hpp btree-rust.h)

target_compile_options(${PROJECT_NAME} PRIVATE
        -Wall -Wextra -Werror
        )

add_compile_options(-fsanitize=address)
add_link_options(-fsanitize=address)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    set(CARGO_FLAGS "--release")
    set(RUST_PATH "${CMAKE_SOURCE_DIR}/target/release/libbtree.a")
else ()
    set(RUST_PATH "${CMAKE_SOURCE_DIR}/target/debug/libbtree.a")
endif ()

add_custom_target(btree_rust_debug ALL
        COMMAND cargo build "${CARGO_FLAGS}"
        BYPRODUCTS ${RUST_PATH}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )

add_dependencies(btree btree_rust_debug)
target_link_libraries(btree ${RUST_PATH} Threads::Threads -ldl)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
cmake_minimum_required(VERSION 3.23)
project(btree)

set(CMAKE_CXX_STANDARD 20)

add_executable(btree test.cpp btree.cpp btree.h PerfEvent.hpp BTreeNode.cpp BTreeNode.h BasicNode.cpp BasicNode.h FatSlot.h util.h)

target_compile_options(${PROJECT_NAME} PRIVATE
        -Wall -Wextra -Werror
        )

add_compile_options(-fsanitize=address)
add_link_options(-fsanitize=address)
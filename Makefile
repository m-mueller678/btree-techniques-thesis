.PHONY: gdb
gdb: btree-debug
	gdb -tui btree-debug

WARN_FLAGS = -Wall -Werror -Wextra

btree-debug: btree.cpp test.cpp
	'clang++' -g -O0 btree.cpp test.cpp -o btree-debug $(WARN_FLAGS) -fsanitize=address

btree-optimized:
	'clang++' -O3 btree.cpp test.cpp -o btree-optimized $(WARN_FLAGS)



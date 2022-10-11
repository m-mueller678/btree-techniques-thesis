.PHONY: gdb
gdb: btree-debug
	gdb -tui btree-debug

btree-debug: btree.cpp test.cpp
	'clang++' -g -O0 btree.cpp test.cpp -o btree-debug

btree-optimized:
	'clang++' -O3 btree.cpp test.cpp -o btree-optimized



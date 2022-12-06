all: tpcc.elf

tpcc.o: newbm.cpp tpcc/*pp
	g++-10 -c -DNDEBUG -DMMAPFILE -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables newbm.cpp -o tpcc.o

../target/release/libbtree.a: always-rebuild
	cd ..;\
	cargo rustc --lib --release -- -C target-cpu=cascadelake

.PHONY: always-rebuild

tpcc.elf: ../target/release/libbtree.a tpcc.o
	 g++-10  tpcc.o -o tpcc.elf -lpthread -ldl -lm -ltbb -laio  -L../target/release/ -lbtree

clean:
	rm tpcc.o tpcc

vmcache: newbm.cpp tpcc/*pp
	g++-10 -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables newbm.cpp -o vmcache -lpthread -ldl -lm -ltbb -laio

mmapfile: newbm.cpp tpcc/*pp
	g++-10 -DNDEBUG -DMMAPFILE -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables newbm.cpp -o mmapfile -lpthread -ldl -lm -ltbb -laio

all: vmcache mmapfile

clean:
	rm vmcache mmapfile

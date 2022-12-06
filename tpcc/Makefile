vmcache: newbm.cpp tpcc/*pp
	g++ -DNDEBUG -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables newbm.cpp -o vmcache -ltbb -laio

mmapfile: newbm.cpp tpcc/*pp
	g++ -DNDEBUG -DMMAPFILE -O3 -std=c++20 -g -fnon-call-exceptions -fasynchronous-unwind-tables newbm.cpp -o mmapfile -ltbb -laio

all: vmcache mmapfile

clean:
	rm vmcache mmapfile

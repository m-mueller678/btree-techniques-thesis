#!/bin/bash

clang++ -O3 btree2020.cpp test.cpp -o btree-perf
ssh cascade-01 rm -rf cp-target
rsync -E -e ssh btree-perf cascade-01:~/cp-target
rm btree-perf
ssh -t cascade-01 SHUF=1 INT=10e6 LONG1=900 LONG2=900 './cp-target'
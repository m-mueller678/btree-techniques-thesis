#!/bin/bash

clang++ -fprofile-instr-generate -fcoverage-mapping -O3 btree2020.cpp test.cpp -o cov
LLVM_PROFILE_FILE="cov.profraw" SHUF=1 INT=10e6 LONG1=900 LONG2=900 FILE=urlsuniqshufmedium ./cov
llvm-profdata merge -sparse cov.profraw -o cov.profdata
llvm-cov show ./cov -instr-profile=cov.profdata
llvm-cov report ./cov -instr-profile=cov.profdata

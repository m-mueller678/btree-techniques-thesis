#!/bin/bash

clang++ -fprofile-instr-generate -fcoverage-mapping -O3 btree2020.cpp -o cov
LLVM_PROFILE_FILE="cov.profraw" ./cov ../btree/urlsuniqshuf
llvm-profdata merge -sparse cov.profraw -o cov.profdata
llvm-cov show ./a.out -instr-profile=cov.profdata

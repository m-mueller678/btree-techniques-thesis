#!/bin/bash

ssh cascade-01 rm -r cp-target
rsync -E -e ssh cmake-build-release/btree cascade-01:~/cp-target
ssh -t cascade-01 NAME="$1" SHUF=1 FILE=data/urls './cp-target'

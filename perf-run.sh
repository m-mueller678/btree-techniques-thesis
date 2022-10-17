#!/bin/bash

ssh cascade-01 rm -rf cp-target
rsync -E -e ssh cmake-build-release/btree cascade-01:~/cp-target
ssh -t cascade-01 SHUF=1 INT=1e8 './cp-target'
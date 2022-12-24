#!/bin/bash

ssh cascade-01 rm -r btree
cargo rustc --bin btree --release -- -C target-cpu=cascadelake
rsync target/release/btree cascade-01:
ssh -t cascade-01 OP_COUNT=1e8 INT=2e7 './btree'

This repository contains code used for measuring the performance impact of various optimization techniques on a B-Tree implementation.
It conists roughly of the following:
- the btree implementation as a cargo project at the repository root
- `configure.py` which controls the build process
- a tpcc implementation adapted from LeanStore
- Legacy C++ and CMake related files which are mostly obslete

`Cargo.toml` and `build-info.h` are partially and fully generated by `configure.py` respectively.

This is explicitly not a production ready B-Tree.

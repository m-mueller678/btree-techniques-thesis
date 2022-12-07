cd tpcc
make
rsync tpcc.elf cascade-01:
ssh -o SendEnv=RUNFOR cascade-01 ./tpcc.elf 1 $WAREHOUSES
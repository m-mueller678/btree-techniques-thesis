cd tpcc
make
rsync tpcc.elf zen4-students:
ssh zen4-students 'env RUNFOR='$RUNFOR' ./tpcc.elf 1 '$WAREHOUSES &
rsync tpcc.elf cascade-01:
ssh cascade-01 'env RUNFOR='$RUNFOR' ./tpcc.elf 1 '$WAREHOUSES &
set -e

OUT="$HOME/tpcc-adapt-leave-noconvert.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..3}
do
RUNFOR=750 find . -name 'btree-*' -exec {} 1 100 >> $OUT \;
done


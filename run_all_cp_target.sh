set -e

OUT="$HOME/tpcc-2.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..10}
do
RUNFOR=750 find . -name 'btree-*' -exec {} 1 100 >> $OUT \;
done


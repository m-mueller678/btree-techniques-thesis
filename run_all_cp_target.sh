set -e

OUT="$HOME/tpcc.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..10}
do
RUNFOR=300 find . -name 'btree-*' -exec {} 1 250 >> $OUT \;
done

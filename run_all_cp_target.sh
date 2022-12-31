set -e

OUT="$HOME/hash-range-ratio.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in `seq 0 4 100`
do
ri=$(echo \(100-$i\)/2 | bc)
export OP_RATES="[0,0,0,$ri,$ri,$i]"
OP_COUNT=4e7 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=4e7 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done

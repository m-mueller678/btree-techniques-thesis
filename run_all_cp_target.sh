set -e

OUT="$HOME/advanced.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..10}
do
OP_COUNT=1e8 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e8 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done

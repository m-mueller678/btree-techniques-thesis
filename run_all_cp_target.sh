set -e

OUT="$HOME/adapt-leave-time.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..5}
do
OP_COUNT=1e8 INT=2E7 find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e8 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
done
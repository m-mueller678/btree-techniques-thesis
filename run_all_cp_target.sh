set -e

OUT="$HOME/adapt-inner.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..20}
do
OP_COUNT=1e8 FILE="$HOME/data/genome" find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e8 FILE="$HOME/data/wiki" find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e8 FILE="$HOME/data/access" find . -name 'btree-*' -exec {} >> $OUT \;
done

OUT="$HOME/adapt-inner-empty.out"

for i in {1..50}
do
OP_RATES='[0,0,0,1,0,0]' START_EMPTY=1 OP_COUNT=6e6 INT="2E7" find . -name 'btree-*' -exec {} >> $OUT \;
OP_RATES='[0,0,0,1,0,0]' START_EMPTY=1 OP_COUNT=6e6 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
OP_RATES='[0,0,0,1,0,0]' START_EMPTY=1 OP_COUNT=6e6 FILE="$HOME/data/wiki" find . -name 'btree-*' -exec {} >> $OUT \;
OP_RATES='[0,0,0,1,0,0]' START_EMPTY=1 OP_COUNT=6e6 FILE="$HOME/data/access" find . -name 'btree-*' -exec {} >> $OUT \;
done

set -e

OUT="$HOME/adaptive-inner-tracked-$(date '+%Y-%m-%d-%H-%M-%S').csv"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

OP_COUNT=1e9 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e9 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e9 LOCALITY=0.99 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;

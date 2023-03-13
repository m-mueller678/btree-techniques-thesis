set -e

OUT="$HOME/leave-adapt-detail.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

RUNFOR=750 find . -name 'btree-*' -exec {} 1 100 >> $OUT 2>&1 \;
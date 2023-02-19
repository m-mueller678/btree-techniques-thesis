set -e

OUT="$HOME/leave-adapt-detail.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

RUNFOR=900 find . -name 'btree-*' -exec {} 1 250 >> $OUT 2>&1 \;

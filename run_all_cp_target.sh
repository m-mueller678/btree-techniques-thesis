set -e

OUT="$HOME/head-tag-counts.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;

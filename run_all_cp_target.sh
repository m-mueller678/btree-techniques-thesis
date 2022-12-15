set -e

OUT="$HOME/common_out.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..40}
do
OP_COUNT=1e9 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
OP_COUNT=1e9 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done
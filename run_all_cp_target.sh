set -e

OUT="$HOME/cache_sequential.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..50}
do
FRONT_COUNT=$i IT_COUNT=1e7 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
FRONT_COUNT=$i IT_COUNT=1e7 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done
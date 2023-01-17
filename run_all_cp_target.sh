set -e

OUT="$HOME/cache_sequential_nacc.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..150}
do
FRONT_COUNT=$i IT_COUNT=1e5 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
FRONT_COUNT=$i IT_COUNT=1e5 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done
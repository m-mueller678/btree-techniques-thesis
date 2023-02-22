set -e

OUT="$HOME/leave-adapt-ranges.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..5}
do
RUNFOR=900 find . -name 'btree-*' -exec {} 1 250 >> $OUT 2>&1 \;
done


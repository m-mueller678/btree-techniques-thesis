set -e

OUT="$HOME/basic_opt.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..10}
do
find . -name 'btree-*'| xargs -n1 -P4 env OP_COUNT=1e9 FILE="$HOME/data/urls" >> "$OUT"
find . -name 'btree-*'| xargs -n1 -P4 env OP_COUNT=1e9 INT=2e7 >> "$OUT"
done
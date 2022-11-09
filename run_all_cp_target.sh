set -e

OUT="$HOME/out-$(date '+%Y-%m-%d-%H-%M-%S').csv"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..8}
do
FILE="$HOME/data/urls" SHUF=1 find . -name 'btree-*' -exec {} >> $OUT \;
INT=2e7 SHUF=1 find . -name 'btree-*' -exec {} >> $OUT \;
done

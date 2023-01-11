set -e

OUT="$HOME/cache-heads.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..70}
do
#OP_COUNT=1e8 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
#OP_COUNT=1e8 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
OP_RATES='[1,0,0,0,0,0]' ZIPF_EXPONENT=$(echo "$i *0.1"  | bc ) FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
done

for i in {1..70}
do
#OP_COUNT=1e8 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
#OP_COUNT=1e8 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
OP_RATES='[1,0,0,0,0,0]' ZIPF_EXPONENT=$(echo "$i *0.1"  | bc ) INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done


for i in {1..70}
do
#OP_COUNT=1e8 FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
#OP_COUNT=1e8 INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
ZIPF_EXPONENT=$(echo "$i *0.1"  | bc ) FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
done

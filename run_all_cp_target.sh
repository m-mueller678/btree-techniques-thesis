set -e

OUT="$HOME/fpt-size.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {1..1}
do
FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
FILE="$HOME/data/genome" find . -name 'btree-*' -exec {} >> $OUT \;
FILE="$HOME/data/wiki" find . -name 'btree-*' -exec {} >> $OUT \;
FILE="$HOME/data/access" find . -name 'btree-*' -exec {} >> $OUT \;
INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done

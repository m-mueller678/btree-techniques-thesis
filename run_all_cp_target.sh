set -e

OUT="$HOME/fpt-size-rng.out"
touch $OUT

cd ~/cp-target

find . -name 'btree-*' -exec chmod u+x {}  \;

for i in {0..10}
do
CHUNK=$i FILE="$HOME/data/urls" find . -name 'btree-*' -exec {} >> $OUT \;
CHUNK=$i FILE="$HOME/data/genome" find . -name 'btree-*' -exec {} >> $OUT \;
CHUNK=$i FILE="$HOME/data/wiki" find . -name 'btree-*' -exec {} >> $OUT \;
CHUNK=$i FILE="$HOME/data/access" find . -name 'btree-*' -exec {} >> $OUT \;
CHUNK=$i INT=2e7 find . -name 'btree-*' -exec {} >> $OUT \;
done

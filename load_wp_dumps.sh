#!/bin/bash

DUMP_PATH=$1
DATE=$2
DB=$3

mkdir -p "$DUMP_PATH/$DATE"
cd "$DUMP_PATH/$DATE"

REMOTE=https://dumps.wikimedia.org/enwiki/$DATE

WP_FILE="enwiki-$DATE-pages-meta-current.xml.bz2"
GT_FILE="enwiki-$DATE-geo_tags.sql"
RE_FILE="enwiki-$DATE-redirect.sql"
PG_FILE="enwiki-$DATE-page.sql"
LN_FILE="enwiki-$DATE-pagelinks.sql"

wget $REMOTE/$GT_FILE.gz
wget $REMOTE/$RE_FILE.gz
wget $REMOTE/$PG_FILE.gz
wget $REMOTE/$LN_FILE.gz
wget $REMOTE/$WP_FILE

gunzip -k $GT_FILE.gz
gunzip -k $RE_FILE.gz
gunzip -k $PG_FILE.gz
gunzip -k $LN_FILE.gz

# drop lines with headers and schema define
sed -i '/^INSERT INTO/! d' "$GT_FILE"
sed -i '/^INSERT INTO/! d' "$RE_FILE"
sed -i '/^INSERT INTO/! d' "$PG_FILE"
sed -i '/^INSERT INTO/! d' "$LN_FILE"

#convert quotation
convert() {
    FILE=$1
    COUNT=$2
    echo $FILE
    sed -i 's/`/"/g' "$FILE"
    sed -i 's/\\\"/"/g' "$FILE"
    # HACK: do substitute several times as there are nested \' quatations
    for i in {1 .. $2}; do
        sed -i "s/\\\'/''/g" "$FILE"
    done
}

convert $GT_FILE 1
convert $RE_FILE 3
convert $PG_FILE 10
convert $LN_FILE 8


psql $DB -f $THIS_DIR/wp_schema.sql

#rename tables
sed -i 's/INSERT INTO "geo_tags"/INSERT INTO import.geo_tags/g' $GT_FILE
psql $DB -c 'DELETE from import.geo_tags;'
psql $DB -f $GT_FILE > gt.log

sed -i 's/INSERT INTO "redirect"/INSERT INTO import.redirect/g' $RE_FILE
psql $DB -c 'DELETE from import.redirect;'
psql $DB -f $RE_FILE > re.log

sed -i 's/INSERT INTO "page"/INSERT INTO import.page/g' $PG_FILE
psql $DB -c 'DELETE from import.page;'
psql $DB -f $PG_FILE > pg.log

sed -i 's/INSERT INTO "pagelinks"/INSERT INTO import.pagelinks/g' $LN_FILE
psql $DB -c 'DELETE from import.pagelinks;'
psql $DB -f $LN_FILE > ln.log

#python3 $THIS_DIR/import_wikipedia.py "$DB" $WP_FILE

cd -


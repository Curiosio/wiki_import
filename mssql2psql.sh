#!/bin/bash

GT_FILE=$1
RE_FILE=$2
PG_FILE=$3
LN_FILE=$4
PP_FILE=$5

# drop lines with headers and schema define
# sed -i '/^INSERT INTO/! d' "$GT_FILE"
# sed -i '/^INSERT INTO/! d' "$RE_FILE"
# sed -i '/^INSERT INTO/! d' "$PG_FILE"
# sed -i '/^INSERT INTO/! d' "$LN_FILE"
sed -i '/^INSERT INTO/! d' "$PP_FILE"

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

#convert $GT_FILE 1
#convert $RE_FILE 3
#convert $PG_FILE 6
#convert $LN_FILE 3
convert $PP_FILE 1

#rename tables
#sed -i 's/INSERT INTO "geo_tags"/INSERT INTO wp.geo_tags/g' $GT_FILE
#psql -h graal -p 35432 -Udocker -d gis -c 'DELETE from wp.geo_tags;'

#sed -i 's/INSERT INTO "redirect"/INSERT INTO wp.redirect/g' $RE_FILE
#psql -h graal -p 35432 -Udocker -d gis -c 'DELETE from wp.redirect;'

#sed -i 's/INSERT INTO "page"/INSERT INTO wp.page/g' $PG_FILE
#psql -h graal -p 35432 -Udocker -d gis -c 'DELETE from wp.page;'

# sed -i 's/INSERT INTO "pagelinks"/INSERT INTO wp.pagelinks/g' $LN_FILE
# psql -h graal -p 35432 -Udocker -d gis -c 'DELETE from wp.pagelinks;'

sed -i 's/INSERT INTO "page_props"/INSERT INTO wp.page_props/g' $PP_FILE
psql -h graal -p 35432 -Udocker -d gis -c 'DELETE from wp.page_props;'

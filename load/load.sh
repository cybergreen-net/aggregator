#!/bin/bash

mkdir tmp
python fromS3toRDS.py
echo "Populating tables ..."
psql -h \
cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com \
-U cybergreen \
-d frontend \
-p 5432 \
-c "\COPY count_by_risk FROM tmp/risk.csv WITH delimiter as ',' null '' csv;"

psql -h \
cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com \
-U cybergreen \
-d frontend \
-p 5432 \
-c "\COPY count_by_country FROM tmp/country.csv WITH delimiter as ',' null '' csv;"

psql -h \
cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com \
-U cybergreen \
-d frontend \
-p 5432 \
-c "\COPY count(risk, country, asn, date, period_type, count) FROM tmp/count.csv WITH delimiter as ',' null '' csv;"

rm -R tmp

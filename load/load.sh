#!/bin/bash

TMPDIR=$1
echo "Populating tables ... $TMPDIR/risk.csv"
psql -h \
cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com \
-U cybergreen \
-d frontend \
-p 5432 \
-c "\COPY count_by_risk FROM $TMPDIR/risk.csv WITH delimiter as ',' null '' csv;"

psql -h \
cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com \
-U cybergreen \
-d frontend \
-p 5432 \
-c "\COPY count_by_country FROM $TMPDIR/country.csv WITH delimiter as ',' null '' csv;"

#psql -h \
#cg-stats-dev.crovisjepxcd.eu-west-1.rds.amazonaws.com \
#-U cybergreen \
#-d frontend \
#-p 5432 \
#-c "\COPY count FROM $tmpdir/count.csv WITH delimiter as ',' null '' csv;"
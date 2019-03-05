#!/bin/bash
REGEX=$1'[0-9]+'.csv
COUNTER=0
for FNAME in $( ls $1*.csv ); do
    if [[ $FNAME =~ $REGEX ]]; then
        if [[ $COUNTER -eq 0 ]]; then
            cp $FNAME $1ALL.csv
        else
            tail -n +2 $FNAME >> $1ALL.csv
        fi
        COUNTER=$(( $COUNTER + 1 ))
    fi
done

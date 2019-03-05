#!/bin/bash

COUNTER=1
while true; do
	FNAME=$1$COUNTER.csv
	if [ -r $FNAME ]; then
		if [[ $COUNTER -eq 1 ]]; then
			cp $FNAME $1ALL.csv
		else
			tail -n +2 $FNAME >> $1ALL.csv
		fi
	else
		exit 0
	fi
	COUNTER=$(( $COUNTER + 1 ))
done

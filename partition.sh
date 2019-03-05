#!/bin/bash
head -n $2 $1 > $1.1
CURRENT=2
while true; do
    tail -n +$(( 1 + $2 * ($CURRENT - 1) )) $1 | head -n $2 > $1.$CURRENT
    if [[ ! -s $1.$CURRENT ]]; then
        printf $(( $CURRENT - 1 ))
        exit 0
    fi
    CURRENT=$(( $CURRENT + 1 ))
done

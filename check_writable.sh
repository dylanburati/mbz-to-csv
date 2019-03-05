#!/bin/bash
FAIL=0

[ -d ./csv ] && [ -w ./csv ] && echo './csv/ exists and is writable' || FAIL=1
if [ $FAIL -ne 0 ]; then
    echo './csv/ failed check'
    exit 1
fi

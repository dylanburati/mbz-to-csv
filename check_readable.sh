#!/bin/bash
[ -d ./mbdump ]
    [ -d ./mbdump/mbdump ] &&
        [ -r ./mbdump/mbdump/artist_credit ] &&
        [ -r ./mbdump/mbdump/artist ] &&
        [ -r ./mbdump/mbdump/artist_credit_name ] &&
        [ -r ./mbdump/mbdump/recording ] &&
        [ -r ./mbdump/mbdump/release_group ] &&
        [ -r ./mbdump/mbdump/release_group_secondary_type_join ] &&
        echo './mbdump/mbdump/ has necessary files' ||
        echo './mbdump/mbdump/ failed check'

[ -d ./mbdump-derived ] &&
    [ -d ./mbdump-derived/mbdump ] &&
        [ -r ./mbdump-derived/mbdump/artist_tag ] &&
        echo './mbdump-derived/mbdump/ has necessary files' ||
        echo './mbdump-derived/mbdump/ failed check'


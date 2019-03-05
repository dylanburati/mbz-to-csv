#!/bin/bash
LATESTDIR=$(wget -O - "http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST" | head -n 1)

wget http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/$LATESTDIR/mbdump.tar.bz2
wget http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/$LATESTDIR/mbdump-derived.tar.bz2
[ -d ./mbdump ] && mv mbdump mbdump_backup
[ -d ./mbdump-derived ] && mv mbdump-derived mbdump-derived_backup
mkdir mbdump
cd mbdump
tar xjf ../mbdump.tar.bz2
cd ..
mkdir mbdump-derived
cd mbdump-derived
tar xjf ../mbdump-derived.tar.bz2
cd ..

## mbz-to-csv
Converts a MusicBrainz database dump into two CSV files (albums and songs).
The song data is taken from the `recording` table, and the output contains
only the artist's name, the song name, and the unique ID.
The album data is taken from the `release_group` table, and the output contains
only the artist's name, the name of the album, the unique ID, and the album type
(LP, EP, live recording, etc.)

The output files are currently used by [Relisten](https://relisten.xyz), a
website I created where people rank music and share their lists. Using a lookup
table to get the ID of albums and songs solves the problem of inconsistent
spelling/capitalization, and allows multiple lists to be combined without
manually checking for duplicate rows.

Any feedback is welcome!

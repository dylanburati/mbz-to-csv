#!/usr/bin/env python3
import sys
from typing import *
from sql_to_columns import sql_to_columns
import subprocess
import pandas as pd
import numpy as np

PATH_TO_SHELL = "/bin/bash"


def read_list_from_file(filename: str) -> List:
    fp = open(filename, "r", encoding="utf-8")
    content = "".join(fp.readlines())
    fp.close()
    l = eval(content)
    if type(l) != list:
        raise ValueError("Expected file contents to be a list")
    return l


def pull_database():
    """
    Gets latest version of MusicBrainz database, and extracts data to ./mbdump and ./mbdump-derived

    :return: None
    """
    global PATH_TO_SHELL
    mbz_update_proc = subprocess.run([PATH_TO_SHELL, "./update_db.sh"])


def generate_csv(dcols):
    """
    Generates 2 final .csv files, one based on the `recording` MusicBrainz table, and the other based on `release_group`
    
    Columns for recording.csv (artist_credit: str, name: str, id: int)
    Columns for release_group.csv (artist_credit: str, name: str, id: int, type: int)
    :return: None
    """
    global PATH_TO_SHELL
    artist_credit = pd.read_csv("./mbdump/mbdump/artist_credit", encoding="utf-8", header=None, delimiter="\t",
                                engine="python", quoting=3)
    artist_credit.set_axis(dcols["artist_credit"], axis=1, inplace=True)
    artist_map1 = {}
    for x in artist_credit.itertuples():
        artist_map1[x[1]] = x[2]  # artist_credit.id TO artist_credit.name

    # Use artist tags along with external sources to trim down the data set
    artist_tag = pd.read_csv("./mbdump-derived/mbdump/artist_tag", encoding="utf-8", header=None, delimiter="\t",
                             engine="python", quoting=3)
    artist_tag.set_axis(dcols["artist_tag"], axis=1, inplace=True)
    artist = pd.read_csv("./mbdump/mbdump/artist", encoding="utf-8", header=None, delimiter="\t", engine="python",
                         quoting=3)
    artist.set_axis(dcols["artist"], axis=1, inplace=True)
    artist_map2 = {}
    artist_map2r = {}
    for x in artist.itertuples():
        artist_map2[x[1]] = x[3]   # artist.id TO artist.name
        artist_map2r[str(x[3]).lower()] = x[1]  # lowercase(artist.name) TO artist.id
    artist_name_supplemental = read_list_from_file("./artist_name_supplemental.txt")
    artist_id_supplemental = []
    for x in artist_name_supplemental:
        a = artist_map2r.get(x.lower(), None)
        if a is not None:
            artist_id_supplemental.append(a)

    artists_with_tag = artist_tag.loc[:, "artist"].drop_duplicates()
    artists_with_tag = pd.concat((artists_with_tag, pd.Series(artist_id_supplemental)))
    artists_with_tag.drop_duplicates(inplace=True)

    artist_credit_name = pd.read_csv("./mbdump/mbdump/artist_credit_name", encoding="utf-8", header=None, delimiter="\t", engine="python", quoting=3)
    artist_credit_name.set_axis(dcols["artist_credit_name"], axis=1, inplace=True)

    artist_map3 = {}
    for x in artist_credit_name.itertuples():
        cr = artist_map3.get(x[3], [])
        cr.append(x[1])
        artist_map3[x[3]] = cr  # artist.id TO artist_credit.id (multiple)
    artist_map4 = {}
    for x in artists_with_tag.iteritems():
        for cr in artist_map3.get(x[1], []):
            an = artist_map1.get(cr, None)
            if an is not None:
                artist_map4[cr] = an  # artist_credit.id TO artist.name (not unique)

    # Partition recording data to avoid out-of-memory
    partition_proc = subprocess.run([PATH_TO_SHELL, "./partition.sh", "./mbdump/mbdump/recording", "3000000"],
                                    stdout=subprocess.PIPE)
    partition_count = int(str(partition_proc.stdout, encoding="utf-8"))
    print("generate_csv_recording: generating {} intermediate files".format(partition_count))

    def convert_to_artist(ac):
        try:
            i = int(ac)
            return artist_map4.get(i, np.nan)
        except ValueError:
            return np.nan

    # Trim each set of recording data to `artist_credit` in `artist_map4`, remove duplicates based on
    # case-sensitive comparisons of ordered pair (artist_credit.name, recording.name)
    for i in range(1, partition_count + 1):
        recording1 = pd.read_csv("./mbdump/mbdump/recording.{}".format(i), encoding="utf-8", header=None,
                                 delimiter="\t", engine="python", quoting=3, converters={3: convert_to_artist})
        recording1.set_axis(dcols["recording"], axis=1, inplace=True)
        recording1_disp = recording1.loc[:, ["artist_credit", "name"]]
        recording1_disp["artist_credit"] = recording1_disp["artist_credit"].str.lower()
        recording1_disp["name"] = recording1_disp["name"].str.lower()
        recording1_disp.dropna(inplace=True)
        recording1_disp.drop_duplicates(inplace=True)
        recording1 = recording1.reindex(recording1_disp.index)
        recording1 = recording1.loc[:, ["name", "artist_credit", "id"]]
        recording1.to_csv("./csv/recording{}.csv".format(i), index=False)
        print("generate_csv_recording: recording{0:d}.csv, {1:d} rows".format(i, recording1.shape[0]))

    # Garbage collection
    if partition_count >= 1:
        del recording1
        del recording1_disp
    del artist_credit
    del artist_tag
    del artist
    del artists_with_tag
    del artist_credit_name
    del artist_map2
    del artist_map2r
    del artist_map3

    # Concatenate trimmed data
    rejoin_proc = subprocess.run([PATH_TO_SHELL, "./rejoin_numeric.sh", "./mbdump/mbdump/recording", "./csv/recording"])

    recording_all = pd.read_csv("./csv/recordingALL.csv", encoding="utf-8")
    recording_all_disp = recording_all.loc[:, ["artist_credit", "name"]]
    recording_all_disp["artist_credit"] = recording_all["artist_credit"].str.lower()
    recording_all_disp["name"] = recording_all["name"].str.lower()
    recording_all_disp = recording_all_disp[~recording_all_disp["name"].str.contains("\x6e\x69\x67\x67\x65\x72")]
    recording_all_disp.drop_duplicates(inplace=True)
    recording_all = recording_all.reindex(recording_all_disp.index)
    recording_all.to_csv("./csv/recording.csv", index=False)
    print("generate_csv_recording: recording.csv, {0:d} rows".format(recording_all.shape[0]))

    # Garbage collection
    del recording_all
    del recording_all_disp

    # End of generate_csv_recording
    # Start of generate_csv_release_group
    def convert_to_artist2(ac):
        try:
            i = int(ac)
            return artist_map1.get(i, np.nan)
        except ValueError:
            return np.nan

    # Generate release type map
    release_group = pd.read_csv("./mbdump/mbdump/release_group", encoding="utf-8", header=None, delimiter="\t",
                                engine="python", quoting=3, converters={3: convert_to_artist2})
    release_group.set_axis(dcols["release_group"], axis=1, inplace=True)
    release_group_type2 = pd.read_csv("./mbdump/mbdump/release_group_secondary_type_join", header=None, delimiter="\t",
                                      engine="python", quoting=3)
    release_group_type2.set_axis(dcols["release_group_secondary_type_join"], axis=1, inplace=True)
    release_type_map = {}
    for x in release_group.itertuples():
        release_type_map[x[1]] = x[5]  # release_group.id TO release_group.type
    for x in release_group_type2.itertuples():
        release_type_map[x[1]] = str(20 + x[2])  # supersede (live version or rerecording of release_group.id)

    # Index by id and update type
    release_group.set_axis(release_group.loc[:, "id"].tolist(), axis=0, inplace=True)
    release_group["type"] = pd.Series(release_type_map)
    release_group["type"] = release_group["type"].str.replace(r"\\N", "-1")

    # Remove duplicates based on case-sensitive comparisons of ordered pair (artist_credit.name, release_group.name)
    release_group_disp = release_group.loc[:, ["artist_credit", "name"]]
    release_group_disp["artist_credit"] = release_group_disp["artist_credit"].str.lower()
    release_group_disp["name"] = release_group_disp["name"].str.lower()
    release_group_disp.dropna(inplace=True)
    release_group_disp.drop_duplicates(inplace=True)
    release_group_disp = release_group_disp[~release_group_disp["name"].str.contains("\x6e\x69\x67\x67\x65\x72")]
    release_group_disp = release_group_disp[~release_group_disp["artist_credit"].str.contains("\x6e\x69\x67\x67\x65\x72")]
    release_group = release_group.reindex(release_group_disp.index)
    release_group = release_group.loc[:, ["name", "artist_credit", "id", "type"]]
    release_group.to_csv("./csv/release_group.csv", index=False)
    print("generate_csv_release_group: release_group.csv, {0:d} rows".format(release_group.shape[0]))


if __name__ == "__main__":
    check_proc1 = subprocess.run([PATH_TO_SHELL, "./check_writable.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(str(check_proc1.stderr, "utf-8"))
    print(str(check_proc1.stdout, "utf-8"))

    if not "--auto" in sys.argv:
        confirm = input("Continue? [Y/n]: ")
        if confirm.capitalize() != "Y":
            sys.exit(0)
    else:
        check_proc1.check_returncode()

    if not "--local" in sys.argv:
        pull_database()

    check_proc2 = subprocess.run([PATH_TO_SHELL, "./check_readable.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(str(check_proc2.stderr, "utf-8"))
    print(str(check_proc2.stdout, "utf-8"))

    if not "--auto" in sys.argv:
        confirm = input("Continue? [Y/n]: ")
        if confirm.capitalize() != "Y":
            sys.exit(0)
    else:
        check_proc2.check_returncode()

    tables_to_columns = sql_to_columns()
    generate_csv(tables_to_columns)

#!/usr/bin/env python3
import sys
from typing import *
from sql_to_columns import sql_to_columns
import subprocess
import csv

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


class CustomDict(dict):
    def __init__(self, key_column: str, val_column: str, all_columns: List[str], from_dict: Dict = None):
        if from_dict is None:
            from_dict = {}
        super().__init__(from_dict)
        self.key_column = all_columns.index(key_column)
        self.val_column = all_columns.index(val_column)

    def parse_and_insert(self, line: str, key_converter: Callable = lambda x: x, val_converter: Callable = lambda x: x) -> None:
        k = key_converter(line[self.key_column])
        v = val_converter(line[self.val_column])
        if k is not None and v is not None:
            self[k] = v

    def parse_and_update(self, line: str, key_converter: Callable = lambda x: x, val_updater: Callable = lambda prev, x: x) -> None:
        k = key_converter(line[self.key_column])
        if k is not None:
            prev = self.get(k, None)
            v = val_updater(prev, line[self.val_column])
            if v is not None:
                self[k] = v


class CustomSet(set):
    def __init__(self, val_column: str, all_columns: List[str], from_list: Iterable = None):
        if from_list is None:
            from_list = []
        super().__init__(from_list)
        self.val_column = all_columns.index(val_column)

    def parse_and_add(self, line: str, val_converter: Callable = lambda x: x) -> None:
        v = val_converter(line[self.val_column])
        if v is not None:
            self.add(v)


class HashTable(dict):
    def __init__(self, use_columns: List[str], all_columns: List[str]):
        super().__init__()
        self.labels = use_columns
        self.use_columns = [all_columns.index(col) for col in use_columns]

    def parse_and_add(self, line: str, hash_gen: Callable, val_converters: Dict[str, Callable]) -> None:
        row = [None] * len(self.use_columns)
        for i, src_idx, label in zip(range(len(self.use_columns)), self.use_columns, self.labels):
            converter = val_converters.get(label, None)
            if converter is None:
                row[i] = line[src_idx]
            else:
                row[i] = converter(line[src_idx])
        k = hash_gen(row)
        if k is not None and k not in self:
            self[k] = row


def generate_csv(dcols):
    """
    Generates 2 final .csv files, one based on the `recording` MusicBrainz table, and the other based on `release_group`

    Columns for recording.csv (artist_credit: str, name: str, id: int)
    Columns for release_group.csv (artist_credit: str, name: str, id: int, type: int)
    :return: None
    """
    global PATH_TO_SHELL

    artist_map1 = CustomDict("id", "name", dcols["artist_credit"])
    with open("./mbdump/mbdump/artist_credit", "r", encoding="utf-8") as fp:
        artist_credit = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in artist_credit:
            artist_map1.parse_and_insert(line)

    artist_map2 = CustomDict("name", "id", dcols["artist"])
    with open("./mbdump/mbdump/artist", "r", encoding="utf-8") as fp:
        artist = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in artist:
            artist_map2.parse_and_insert(line, key_converter=str.lower)

    artist_name_supplemental = read_list_from_file("./artist_name_supplemental.txt")
    artist_id_supplemental = []
    for x in artist_name_supplemental:
        a = artist_map2.get(x.lower(), None)
        if a is not None:
            artist_id_supplemental.append(a)
    del artist_map2

    artist_map3 = CustomDict("artist", "artist_credit", dcols["artist_credit_name"])
    with open("./mbdump/mbdump/artist_credit_name", "r", encoding="utf-8") as fp:
        artist_credit_name = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in artist_credit_name:
            def val_updater(prev, x):
                if prev is None:
                    return [x]
                prev.append(x)
                return prev
            artist_map3.parse_and_update(line, val_updater=val_updater)

    artists_with_tag = CustomSet("artist", dcols["artist_tag"], from_list=artist_id_supplemental)
    with open("./mbdump-derived/mbdump/artist_tag", "r", encoding="utf-8") as fp:
        artist_tag = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in artist_tag:
            artists_with_tag.parse_and_add(line)

    artist_map4 = {}
    for a in artists_with_tag:
        associated_credits = artist_map3.get(a, [])
        for ac in associated_credits:
            name = artist_map1.get(ac, None)
            if name is not None:
                artist_map4[ac] = name

    print('total artists: {}, tagged+supplemental: {}'.format(len(artist_map1), len(artist_map4)), flush=True)
    del artist_map3

    recording_use_columns = ["id", "artist_credit", "name"]

    def convert_to_artist(x):
        return artist_map4.get(x, None)

    def normalize_name(x):
        charmap = [
            ('\u2010', '-'),
            ('\u2011', '-'),
            ('\u2012', '-'),
            ('\u2013', '-'),
            ('\u2014', '-'),
            ('\u2018', "'"),
            ('\u2019', "'"),
            ('\u201c', '"'),
            ('\u201d', '"'),
            ('\u2026', '...')
        ]
        for ch, rch in charmap:
            x = x.replace(ch, rch)
        return x

    def recording_hash(row):
        if row[1] is None or row[2] is None or row[1] == "" or row[2] == "" \
                or "\x6e\x69\x67\x67\x65\x72" in row[1].lower() or "\x6e\x69\x67\x67\x65\x72" in row[2].lower():
            return None
        return hash(row[1].lower() + row[2].lower())

    recording_all = HashTable(recording_use_columns, dcols["recording"])
    with open("./mbdump/mbdump/recording", "r", encoding="utf-8") as fp:
        recording = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in recording:
            recording_all.parse_and_add(line, recording_hash, {"artist_credit": convert_to_artist, "name": normalize_name})

    print('recording.csv: writing {} rows'.format(len(recording_all)), flush=True)
    with open("./csv/recording.csv", "w", newline="", encoding="utf-8") as fp:
        fp.write(",".join(recording_use_columns) + "\r\n")
        recording_out = csv.writer(fp, quoting=csv.QUOTE_MINIMAL)
        for row in recording_all.values():
            recording_out.writerow(row)
    print('recording.csv written', flush=True)

    del recording_all
    # End of generate_csv_recording
    # Start of generate_csv_release_group
    release_group_use_columns = ["id", "artist_credit", "name", "type"]

    def convert_to_artist2(x):
        return artist_map1.get(x, None)

    def convert_type(x):
        if x == r'\N':
            return '-1'
        return x

    def release_group_hash(row):
        if row[1] is None or row[2] is None or row[1] == "" or row[2] == "" \
                or "\x6e\x69\x67\x67\x65\x72" in row[1].lower() or "\x6e\x69\x67\x67\x65\x72" in row[2].lower():
            return None
        return hash(row[1].lower() + row[2].lower())

    release_group_all = HashTable(release_group_use_columns, dcols["release_group"])
    with open("./mbdump/mbdump/release_group", "r", encoding="utf-8") as fp:
        release_group = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in release_group:
            release_group_all.parse_and_add(line, release_group_hash, {"artist_credit": convert_to_artist2,
                                                                       "type": convert_type, "name": normalize_name})

    release_type_map = CustomDict("release_group", "secondary_type", dcols["release_group_secondary_type_join"])
    with open("./mbdump/mbdump/release_group_secondary_type_join", "r", encoding="utf-8") as fp:
        release_group_type2 = csv.reader(fp, delimiter="\t", quoting=csv.QUOTE_NONE)
        for line in release_group_type2:
            release_type_map.parse_and_insert(line)

    print('release_group.csv: writing {} rows'.format(len(release_group_all)), flush=True)
    with open("./csv/release_group.csv", "w", newline="", encoding="utf-8") as fp:
        fp.write(",".join(release_group_use_columns) + "\r\n")
        release_group_out = csv.writer(fp, quoting=csv.QUOTE_MINIMAL)
        for row in release_group_all.values():
            type2 = release_type_map.get(row[0], None)
            if type2 is not None:
                row[3] = str(20 + int(type2))
            release_group_out.writerow(row)
    print('release_group.csv written', flush=True)


if __name__ == "__main__":
    check_proc1 = subprocess.run([PATH_TO_SHELL, "./check_writable.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(str(check_proc1.stderr, "utf-8"), flush=True)
    print(str(check_proc1.stdout, "utf-8"), flush=True)

    if not "--auto" in sys.argv:
        confirm = input("Continue? [Y/n]: ")
        if confirm.capitalize() != "Y":
            sys.exit(0)
    else:
        check_proc1.check_returncode()

    if not "--local" in sys.argv:
        pull_database()

    check_proc2 = subprocess.run([PATH_TO_SHELL, "./check_readable.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(str(check_proc2.stderr, "utf-8"), flush=True)
    print(str(check_proc2.stdout, "utf-8"), flush=True)

    if not "--auto" in sys.argv:
        confirm = input("Continue? [Y/n]: ")
        if confirm.capitalize() != "Y":
            sys.exit(0)
    else:
        check_proc2.check_returncode()

    tables_to_columns = sql_to_columns()
    generate_csv(tables_to_columns)

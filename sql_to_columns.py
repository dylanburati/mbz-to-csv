#!/usr/bin/env python3
from typing import *
import requests
import re


def read_to_delimiter(s: str, start_idx: int, delimiter: str, inclusive: bool = False) -> Tuple[str, int]:
    end_idx = s.find(delimiter, start_idx)
    if end_idx == -1:
        return (s[start_idx:], -1)
    if inclusive:
        end_idx += len(delimiter)
    return (s[start_idx:end_idx], end_idx)


def read_to_regex_match(s: str, start_idx: int, regex: str) -> Tuple[str, str, int]:
    match = re.search(regex, s[start_idx:], re.M)
    if not match:
        return (s[start_idx:], None, -1)
    start_match_idx = start_idx + match.span()[0]
    end_idx = start_idx + match.span()[1]
    return (s[start_idx:start_match_idx], match.group(), end_idx)


def skip_optional(s: str, start_idx: int, text: str) -> int:
    if s[start_idx:].startswith(text):
        return start_idx + len(text)
    return start_idx


def skip_optional_regex(s: str, start_idx: int, regex: str) -> int:
    match = re.match(regex, s[start_idx:], re.M)
    if not match:
        return start_idx
    return start_idx + match.span()[1] - match.span()[0]


def sql_to_columns() -> Dict[str, List[str]]:
    """
    Pulls the latest table structures from the MusicBrainz GitHub and returns the columns for each table.

    :return: tables_to_columns
    """
    sql_commented = requests.get(
        'https://raw.githubusercontent.com/metabrainz/musicbrainz-server/master/admin/sql/CreateTables.sql').text
    sql_lines = sql_commented.split('\n')
    for i in range(len(sql_lines)):
        sql_lines[i] = read_to_regex_match(sql_lines[i], 0, r'(--|$)')[0]
    sql = '\n'.join(sql_lines)
    ignore_col_names = ['CONSTRAINT', 'INDEX', 'KEY', 'UNIQUE', 'PRIMARY', 'FULLTEXT', 'SPATIAL', 'CHECK']

    tables_to_columns = {}
    start_idx = 0
    nesting = {'CREATE': 0, 'parens': 0}
    current_table = None
    current_token_id = 0
    next_token_id = 0
    while start_idx >= 0:
        if nesting['CREATE'] == 0:
            _discard_, start_idx = read_to_delimiter(sql, start_idx, 'CREATE TABLE ', inclusive=True)
            if start_idx >= 0:
                start_idx = skip_optional(sql, start_idx, 'IF NOT EXISTS ')
                current_table, _discard_, start_idx = read_to_regex_match(sql, start_idx, r'[ \t\n]+')
                _discard_, _discard2_, start_idx = read_to_regex_match(sql, start_idx, r'[(]')
                tables_to_columns[current_table] = []
                nesting['CREATE'] = 1
        else:
            if nesting['parens'] == 0:
                start_idx = skip_optional_regex(sql, start_idx, r'[ \t\n]+')
                token, boundary, start_idx = read_to_regex_match(sql, start_idx, r'[(,)]')
                if next_token_id == current_token_id:
                    next_token_id += 1
                    col_name, _discard_, _discard2_ = read_to_regex_match(token, 0, r'[ \t\n]+')
                    if col_name not in ignore_col_names:
                        tables_to_columns[current_table].append(col_name)
                if boundary == '(':
                    nesting['parens'] = 1
                elif boundary == ',':
                    current_token_id += 1
                elif boundary == ')':
                    current_token_id += 1
                    nesting['CREATE'] = 0
            else:
                _discard_, boundary, start_idx = read_to_regex_match(sql, start_idx, r'[()]')
                if boundary == '(':
                    nesting['parens'] += 1
                elif boundary == ')':
                    nesting['parens'] -= 1
    return tables_to_columns

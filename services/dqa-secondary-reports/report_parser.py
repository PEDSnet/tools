#!/usr/bin/env python3

# Parser for DQA secondary reports

import csv
import json
import sys
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# PEDSnet-v2-Colorado-ETLv2 has data model version listed as '2'
VERSION_MAP = {'v1': '1.0.0',
               'v2': '2.0.0',
               '2': '2.0.0'}

# take care of the known typos.
# Not sure if there's a better way to do this.
STATUS_MAP = {'soltution proposed': 'solution proposed',
              'solutoin proposed': 'solution proposed'}


class Parser():
    def __init__(self, file_obj,
                 hospital, table,
                 field_totals, table_totals, site_totals):
        self.file_obj = file_obj
        self.hospital = hospital
        self.table = table
        self.field_totals = field_totals
        self.table_totals = table_totals
        self.site_totals = site_totals
        self.parsed = False

    def parse(self):
        if self.parsed:
            return self.field_totals

        reader = csv.reader(self.file_obj)

        # skip the header
        next(reader)

        for rec in reader:
            if not rec[7]:
                continue

            field = rec[5].strip().lower()
            code = rec[7].strip().upper()
            rank = rec[11].strip().lower()
            status = rec[14].strip().lower() or 'not specified'

            if status in STATUS_MAP:
                status = STATUS_MAP[status]

            # goal and description are specified in rec[6] and rec[8]
            # respectively, but it's better to pull them from
            # Data-Quality/Dictionary/DCC_DQA_Dictionary.csv
            # to ensure consistensy and no typos

            site_record = {
                'site': self.hospital,
                'rank': rank
            }

            self.field_totals.setdefault(self.table, {}).\
                setdefault(field, {}).setdefault(code, {}).\
                setdefault(status, []).append(site_record)

            field_record = {
                'field': field,
                'rank': rank
            }

            self.table_totals.setdefault(self.table, {}).\
                setdefault(code, {}).setdefault(status, {}).\
                setdefault(self.hospital, []).append(field_record)

            if status not in self.site_totals:
                self.site_totals[status] = 1
            else:
                self.site_totals[status] += 1

    @staticmethod
    def get_version(file_obj):
        reader = csv.reader(file_obj)

        try:
            # first line is the header, skip it
            next(reader)

            version = next(reader)[1]
            if version in VERSION_MAP:
                version = VERSION_MAP[version]

            return version

        except Exception:
            return None


def main(fileobj):
    output = Parser(fileobj, 'unknown hospital', 'unknown_table').parse()
    json.dump(output, sys.stdout, indent=4, sort_keys=True)


if __name__ == '__main__':
    # No filename provided, read from stdin.
    if len(sys.argv) == 1:
        main(sys.stdin)
        sys.exit(0)

    with open(sys.argv[1]) as f:
        main(f)

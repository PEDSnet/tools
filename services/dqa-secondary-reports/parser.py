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


class Parser():
    def __init__(self, file_obj, hospital, table, initial_dqa={}):
        self.file_obj = file_obj
        self.hospital = hospital
        self.table = table
        self.results = initial_dqa
        self.parsed = False

    def parse(self):
        # reader = csv.reader(self.file_obj)
        # Thie line above didn't work, was getting the following error
        # (_csv.Error: new-line character seen in unquoted field - do you need
        #  to open the file in universal-newline mode?).
        # This might have something to do with the way Excel on Mac saves csv
        # files, not sure.
        # Using splitlines() to process the file solved this.

        data = [row for row in csv.reader(self.file_obj.read().splitlines())]

        if self.parsed:
            return self.results

        # skip the header
        for rec in data[1:]:
            if not rec[7]:
                continue

            field = rec[5].strip().lower()
            # goal = rec[6].strip().lower()
            code = rec[7].strip().upper()
            # desc = rec[8]
            rank = (rec[11] or 'not recorded').strip().lower()
            status = (rec[14] or 'not recorded').strip().lower()
            site_record = {'site': self.hospital, 'rank': rank}
            if status not in self.results.setdefault(self.table, {}).\
                    setdefault(field, {}).setdefault(code, {}):
                self.results[self.table][field][code][status] =\
                    [site_record]
            else:
                self.results[self.table][field][code][status].append(site_record)

        return self.results


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

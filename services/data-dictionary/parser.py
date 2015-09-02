#!/usr/bin/env python3

import openpyxl
import json
import sys


class Parser():
    def __init__(self, file_name):
        self.file_name = file_name
        self.dictionary = {}
        self.parsed = False

    def parse(self):
        if self.parsed:
            return self.dictionary

        wb = openpyxl.load_workbook(filename=self.file_name,
                                    use_iterators=True)
        worksheet_names = wb.get_sheet_names()

        for name in worksheet_names:
            worksheet = wb[name]

            # saw a space in the middle of one of the names. Get rid of those
            name = name.replace(' ', '').lower()

            if (not worksheet['D2'].value or
                    not worksheet['D2'].value.startswith('Site Comments')):
                continue

            row_iter = worksheet.iter_rows()

            # Skipping header rows.
            next(row_iter)
            next(row_iter)

            self.dictionary[name] = {}

            for row in row_iter:
                # Read until the first empty field name.
                if row[0].value is None:
                    break

                field = row[0].value.replace(' ', '').lower()

                self.dictionary[name][field] = {
                    'site_comments': row[3].value or '',
                    'implementation_status': row[4].value or '',
                }

        self.parsed = True

        return self.dictionary


def main(file_name):
    output = Parser(file_name).parse()
    json.dump(output, sys.stdout, indent=4, sort_keys=True)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Please provide full path to \
               annotated data dictionary excel spreadsheet.")
        sys.exit(0)

    main(sys.argv[1])

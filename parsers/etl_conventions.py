#!/usr/bin/env python3

# Parser for PEDSnet ETL conventions document

import json
import re
import sys
from signal import signal, SIGPIPE, SIG_DFL

_field = re.compile(r'^(?P<name>[^\|]*)\|(?P<req>[^\|]*)'
                    r'\|(?P<type>[^\|]*)\|(?P<desc>[^\|]*)'
                    r'(\|(?P<conv>[^\|]*))?\|?$')
_title = re.compile(r'^\#[^\#]+')
_table_header = re.compile(r'^Field\s*\|\s*Required\s*\|')
_table_name = re.compile(r'^\#\#\s*\d+\.\d+(?P<name>[ \w]*)')


def _get_table_name(l):
    return _table_name.match(l).group('name').lower().strip().replace(' ', '_')


def _get_field(l):
    '''
    Read field information. The format is
    'Field |Required | Data Type | Description | PEDSnet Conventions',
    where the 'PEDSnet Conventions' field is optional.
    '''
    field = _field.match(l)

    if not field:
        return None

    name = field.group('name').lower().strip()
    req = field.group('req').lower().strip().startswith('yes')
    data_type = field.group('type').strip()
    desc = field.group('desc').strip()
    conv = field.group('conv')
    if conv:
        conv = conv.strip()
    else:
        conv = ''

    return (name, req, data_type, desc, conv)


class Document():
    def __init__(self, fileobj):
        self.model = {
            'content': '',
            'tables': {}
        }

        self.fileobj = fileobj
        self.parsed = False

    def parse(self):
        if self.parsed:
            return self.model

        self.line = ''

        self.get_content()

        while self.get_next_table():
            pass

        self.parsed = True

        return self.model

    def add_content(self, content):
        self.model['content'] = content

    def add_table(self, table, content=''):
        self.model['tables'][table] = {
            'content': content,
            'fields': {}
        }

    def add_table_content(self, table, content):
        self.model['tables'][table]['content'] = content

    def add_field(self, table, field, required, data_type, desc, conv):
        self.model['tables'][table]['fields'][field] = {
            'required': required,
            'data_type': data_type,
            'description': desc,
            'etl_conventions': conv
        }

    def get_content(self):
        # Skip everything up to and including the title line.
        for line in self.fileobj:
            if _title.match(line):
                break

        # Everything up to the first table is model-level content.
        self.line = ''
        content = []

        for line in self.fileobj:
            if _table_name.match(line):
                self.line = line
                break
            content.append(line)

        self.add_content(''.join(content).strip())

    def get_next_table(self):
        '''
        Read from the currently cached line and up to the start of the next
        table (or EOF), and parse current table's content.
        '''

        # The document should now have the table name line cached.
        # If it doesn't, there are no more tables to read.
        if self.line == '':
            return False

        # Table name line looks something like this: "## 1.1 PERSON".
        # Parse out the actual table name.
        table_name = _get_table_name(self.line)
        self.add_table(table_name)

        # Everything after the table name line and up to the table header
        # 'Field |Required | Data Type | Description | PEDSnet Conventions'
        # is table content.
        content = []
        for line in self.fileobj:
            if _table_header.match(line):
                break

            content.append(line)

        # skip the '--- | --- | --- | --- | ---' line as well
        next(self.fileobj)

        # Read field information.
        for line in self.fileobj:
            field = _get_field(line)

            if field:
                self.add_field(table_name, *field)
            else:
                self.line = line
                break

        # Read the rest of the table contents, whatever comes after
        # the fields specification table, up to the next table.
        if not _table_name.match(self.line):
            content.append(line)
            self.line = ''

            for line in self.fileobj:
                if _table_name.match(line):
                    self.line = line
                    break

                content.append(line)

        self.add_table_content(table_name, ''.join(content).strip())

        return True


def main(fileobj):
    # restore the signal handler for SIGPIPE, to avoid broken pipe error
    # when trying to pipe output of this script
    signal(SIGPIPE, SIG_DFL)

    print(json.dumps(Document(fileobj).parse(), indent=4, sort_keys=True))

if __name__ == '__main__':
    if len(sys.argv) == 1:
        main(sys.stdin)

    with open(sys.argv[1]) as f:
        main(f)

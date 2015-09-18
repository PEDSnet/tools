import io
import os
import sys
import json
import requests
import logging
from requests.exceptions import HTTPError
from flask import Flask, Response
from parser import Parser, logger


# Required mediatype for the Accept header to get the raw content.
GITHUB_RAW_MEDIATYPE = 'application/vnd.github.v3.raw'
GITHUB_DEFAULT_MEDIATYPE = 'application/vnd.github.v3'

# Token for GitHub authorization. Set on startup.
GITHUB_AUTH_TOKEN = None

DEFAULT_TIMEOUT = 10

REPO_URL = 'https://api.github.com/repos/PEDSnet/Data-Quality/contents/SecondaryReports/'

class DqaResource():
    def __init__(self, etl_version, repo_url=REPO_URL):
        self.repo_url = repo_url
        self.etl_version = etl_version

        self.cached_content = {}

        self.content_last_modified = {}
        self.content_etag = {}

        self.results = {}

    def __call__(self):
        "Entrypoint for Flask routing."
        try:
            info = self.parse()
        except HTTPError:
            return 503, ''

        content = json.dumps(info, indent=4, sort_keys=True)

        resp = Response(content)

        resp.headers['Content-Type'] = 'application/json'

        return resp

    def parse(self):
        headers = {
            'Accept': GITHUB_RAW_MEDIATYPE,
            'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
        }

        hospitals = ['Boston', 'CCHMC', 'CHOP', 'Colorado', 'Nationwide',
                     'Nemours', 'Seattle', 'StLouis']
        # etlVersions = ['ETLv1', 'ETLv2', 'ETLv3', 'ETLv4', 'ETLv5']
        tables = ['care_site', 'condition_occurrence', 'death',
                  'drug_exposure', 'drug_strength', 'fact_relationship',
                  'location', 'measurement', 'observation',
                  'observation_period', 'person', 'procedure_occurrence',
                  'provider', 'visit_occurrence', 'visit_payer']

        for table in tables:
            for hospital in hospitals:
                logger.debug(table + ' ' + hospital)
                path = self.get_path(hospital, table)
                if path in self.content_last_modified:
                    headers['If-Modified-Since'] = self.content_last_modified[path]

                if path in self.content_etag:
                    headers['If-None-Match'] = self.content_etag[path]

                resp = requests.get(path,
                                    headers=headers,
                                    timeout=DEFAULT_TIMEOUT)
                # no record for this table and ETL level for this site
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()

                self.content_last_modified[path] = resp.headers['Last-Modified']
                self.content_etag[path] = resp.headers['ETag']

                # content modified (based on the conditional headers).
                if resp.status_code == 200:
                    # Wrap decoded bytes in file-like object.
                    self.cached_content[path] = io.StringIO(resp.text)

                Parser(self.cached_content[path], hospital, table, self.results).parse()

        return self.results

    def get_path(self, hospital, table):
        return (self.repo_url + hospital + '/' + self.etl_version + '/'
                + table + '.csv')

# document_url='https://api.github.com/repos/PEDSnet/Data-Quality/contents/SecondaryReports/StLouis/ETLv3/care_site.csv'
ETLv1 = DqaResource(etl_version='ETLv1')
ETLv2 = DqaResource(etl_version='ETLv2')
ETLv3 = DqaResource(etl_version='ETLv3')
ETLv4 = DqaResource(etl_version='ETLv4')
ETLv5 = DqaResource(etl_version='ETLv5')


# Initialize the flask app and register the routes.
app = Flask(__name__)

app.add_url_rule('/pedsnet/ETLv1', 'ETLv1', ETLv1, methods=['GET'])
app.add_url_rule('/pedsnet/ETLv2', 'ETLv2', ETLv2, methods=['GET'])
app.add_url_rule('/pedsnet/ETLv3', 'ETLv3', ETLv3, methods=['GET'])
app.add_url_rule('/pedsnet/ETLv4', 'ETLv4', ETLv4, methods=['GET'])
app.add_url_rule('/pedsnet/ETLv5', 'ETLv5', ETLv5, methods=['GET'])


if __name__ == '__main__':
    usage = """PEDSnet ETL Conventions Service

    Usage: main.py [--token=<token>] [--host=<host>] [--port=<port>] [--debug]

    Options:
        --help              Display the help.
        --token=<token>     GitHub authorization token.
        --host=<host>       Host of the service.
        --port=<port>       Port of the service [default: 5000].
        --debug             Enable debug output.
    """

    from docopt import docopt

    opts = docopt(usage)

    host = opts['--host']
    port = int(opts['--port'])
    debug = opts['--debug']

    # Set token if defined.
    if opts['--token']:
        GITHUB_AUTH_TOKEN = opts['--token']
    else:
        GITHUB_AUTH_TOKEN = os.environ.get('GITHUB_AUTH_TOKEN')

    if not GITHUB_AUTH_TOKEN:
        print('Authorization token required.')
        sys.exit(1)

    if debug:
        logger.setLevel(logging.DEBUG)

    app.run(host=host, port=port, debug=debug)

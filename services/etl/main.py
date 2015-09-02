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

DEFAULT_TIMEOUT = 5


class ETLConventionsResource():
    def __init__(self, document_url, commits_url, file_path):
        self.document_url = document_url
        self.commits_url = commits_url
        self.file_path = file_path

        self.content_last_modified = None
        self.content_etag = None

        self.commit_last_modified = None
        self.commit_etag = None

        self.cached_model = None
        self.cached_commit = None

    def __call__(self):
        "Entrypoint for Flask routing."
        try:
            model = self.parse_model()
        except HTTPError as e:
            return str(e), 503

        try:
            commit = self.parse_commit()
        except HTTPError as e:
            return str(e), 503

        content = json.dumps({
            'commit': commit,
            'model': model,
        })

        resp = Response(content)

        resp.headers['Content-Type'] = 'application/json'

        return resp

    def parse_model(self):
        headers = {
            'Accept': GITHUB_RAW_MEDIATYPE,
            'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
        }

        if self.content_last_modified:
            headers['If-Modified-Since'] = self.content_last_modified

        if self.content_etag:
            headers['If-None-Match'] = self.content_etag

        resp = requests.get(self.document_url,
                            headers=headers,
                            timeout=DEFAULT_TIMEOUT)

        resp.raise_for_status()

        self.content_last_modified = resp.headers['Last-Modified']
        self.content_etag = resp.headers['ETag']

        # Not modified based on the conditional headers.
        if resp.status_code == 200:
            # Wrap decoded bytes in file-like object.
            buff = io.StringIO(resp.text)

            self.cached_model = Parser(buff).parse()

        return self.cached_model

    def parse_commit(self):
        headers = {
            'Accept': GITHUB_DEFAULT_MEDIATYPE,
            'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
        }

        if self.commit_last_modified:
            headers['If-Modified-Since'] = self.commit_last_modified

        if self.commit_etag:
            headers['If-None-Match'] = self.commit_etag

        resp = requests.get(self.commits_url,
                            params={'path': self.file_path},
                            headers=headers,
                            timeout=DEFAULT_TIMEOUT)

        resp.raise_for_status()

        self.commit_last_modified = resp.headers['Last-Modified']
        self.commit_etag = resp.headers['ETag']

        # Not modified based on the conditional headers.
        if resp.status_code == 200:
            # Get the most recent commit.
            commit = resp.json()[0]

            self.cached_commit = {
                'sha': commit['sha'],
                'date': commit['commit']['committer']['date']
            }

        return self.cached_commit


pedsnet_v2 = ETLConventionsResource(
        document_url='https://api.github.com/repos/PEDSnet/Data_Models/contents/PEDSnet/docs/Pedsnet_CDM_ETL_Conventions.md',  # noqa
        commits_url='https://api.github.com/repos/PEDSnet/Data_Models/commits',  # noqa
        file_path='PEDSnet/docs/Pedsnet_CDM_ETL_Conventions.md')

i2b2_v2 = ETLConventionsResource(
        document_url='https://api.github.com/repos/PEDSnet/Data_Models/contents/i2b2/V2/docs/i2b2_pedsnet_v2_etl_conventions.md',  # noqa
        commits_url='https://api.github.com/repos/PEDSnet/Data_Models/commits',  # noqa
        file_path='i2b2/V2/docs/i2b2_pedsnet_v2_etl_conventions.md')


# Initialize the flask app and register the routes.
app = Flask(__name__)

app.add_url_rule('/pedsnet/2.0.0', 'pedsnet_v2', pedsnet_v2, methods=['GET'])
app.add_url_rule('/i2b2/2.0.0', 'i2b2_v2', i2b2_v2, methods=['GET'])


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

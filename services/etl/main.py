import io
import os
import sys
import json
import requests
from requests.exceptions import HTTPError
from flask import Flask, Response
from parser import Document


app = Flask(__name__)

# Document API URL
DOCUMENT_URL = 'https://api.github.com/repos/PEDSnet/Data_Models/contents/PEDSnet/V2/docs/Pedsnet_CDM_V2_OMOPV5_ETL_Conventions.md'  # noqa
COMMITS_URL = 'https://api.github.com/repos/PEDSnet/Data_Models/commits'
FILE_PATH = 'PEDSnet/V2/docs/Pedsnet_CDM_V2_OMOPV5_ETL_Conventions.md'  # noqa

# Required mediatype for the Accept header to get the raw content.
GITHUB_RAW_MEDIATYPE = 'application/vnd.github.v3.raw'
GITHUB_DEFAULT_MEDIATYPE = 'application/vnd.github.v3'

# Token for GitHub authorization. Set on startup.
GITHUB_AUTH_TOKEN = None

content_last_modified = None
content_etag = None

commit_last_modified = None
commit_etag = None


def parse_model():
    global content_etag, content_last_modified

    headers = {
        'Accept': GITHUB_RAW_MEDIATYPE,
        'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
    }

    if content_last_modified:
        headers['If-Modified-Since'] = content_last_modified

    if content_etag:
        headers['If-None-Match'] = content_etag

    resp = requests.get(DOCUMENT_URL,
                        headers=headers,
                        timeout=5)

    resp.raise_for_status()

    content_last_modified = resp.headers['Last-Modified']
    content_etag = resp.headers['ETag']

    # Wrap decoded bytes in file-like object.
    buff = io.StringIO(resp.text)

    return Document(buff).parse()


def parse_commit():
    global commit_etag, commit_last_modified

    headers = {
        'Accept': GITHUB_DEFAULT_MEDIATYPE,
        'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
    }

    if commit_last_modified:
        headers['If-Modified-Since'] = commit_last_modified

    if commit_etag:
        headers['If-None-Match'] = commit_etag

    resp = requests.get(COMMITS_URL,
                        params={'path': FILE_PATH},
                        headers=headers,
                        timeout=5)

    resp.raise_for_status()

    commit_last_modified = resp.headers['Last-Modified']
    commit_etag = resp.headers['ETag']

    # Get the most recent commit.
    commit = resp.json()[0]

    return {
        'sha': commit['sha'],
        'date': commit['commit']['committer']['date']
    }


@app.route('/', methods=['GET'])
def index():
    try:
        model = parse_model()
    except HTTPError:
        return 503, ''

    try:
        commit = parse_commit()
    except HTTPError:
        return 503, ''

    resp = Response(json.dumps({
        'commit': commit,
        'model': model,
    }))

    resp.headers['Content-Type'] = 'application/json'

    return resp


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

    app.run(host=host, port=port, debug=debug)

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

# Required mediatype for the Accept header to get the raw content.
GITHUB_RAW_MEDIATYPE = 'application/vnd.github.v3.raw'

# Token for GitHub authorization. Set on startup.
GITHUB_AUTH_TOKEN = None


def parse_document():
    headers = {
        'Accept': GITHUB_RAW_MEDIATYPE,
        'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
    }

    resp = requests.get(DOCUMENT_URL,
                        headers=headers,
                        timeout=5)

    resp.raise_for_status()

    # Wrap decoded bytes in file-like object.
    buff = io.StringIO(resp.text)

    return Document(buff).parse()


@app.route('/', methods=['GET'])
def index():
    try:
        output = parse_document()
    except HTTPError:
        return 503, ''

    resp = Response(json.dumps(output))
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

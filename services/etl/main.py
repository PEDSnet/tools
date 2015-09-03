import io
import os
import sys
import json
import requests
import logging
import provenance
from requests.exceptions import HTTPError
from flask import Flask, Response, request
from parser import Parser, logger


# Required mediatype for the Accept header to get the raw content.
GITHUB_RAW_MEDIATYPE = 'application/vnd.github.v3.raw'

# Default mediatype for GitHub responses.
GITHUB_REQUEST_MEDIATYPE = 'application/vnd.github.v3'

# Token for GitHub authorization. Set on startup.
GITHUB_AUTH_TOKEN = None

# Timeout for the request.
REQUEST_TIMEOUT = 5

# The commits endpoint is used to get the commits for a particular file.
GITHUB_COMMITS_URL = 'https://api.github.com/repos/PEDSnet/Data_Models/commits'

# The contents endpoint is used to get the contents of a file at
# a paritcular revision.
GITHUB_CONTENTS_URL = 'https://api.github.com/repos/PEDSnet/Data_Models/contents'  # noqa


def json_defaults(o):
    if isinstance(o, provenance.entity):
        return o.json()

    raise TypeError


def ldjson(prov):
    for msg in prov:
        yield json.dumps(msg.json())
        yield '\n'


def dedupe_commits(commits):
    shas = set()
    filtered = []

    # Evaluate in descending order so the more recent commit is included
    # in the set.
    for c in reversed(commits):
        if c['sha'] in shas:
            continue

        filtered.append(c)
        shas.add(c['sha'])

    filtered.reverse()
    return filtered


# Get all commits for the file.
def get_commits(path):
    headers = {
        'Accept': GITHUB_REQUEST_MEDIATYPE,
        'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
    }

    resp = requests.get(GITHUB_COMMITS_URL,
                        params={'path': path},
                        headers=headers,
                        timeout=REQUEST_TIMEOUT)

    resp.raise_for_status()

    # Commits are in descending order, so we reverse them.
    commits = resp.json()
    commits.reverse()

    # Add the file path to the annotation
    for i, c in enumerate(commits):
        commits[i] = {
            'sha': c['sha'],
            'commit': c['commit'],
            'file_path': path,
            'timestamp': provenance.parse_date(c['commit']['committer']['date']),  # noqa
        }

    return commits


def get_commit(path, ref='master'):
    headers = {
        'Accept': GITHUB_REQUEST_MEDIATYPE,
        'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
    }

    resp = requests.get(GITHUB_COMMITS_URL,
                        params={
                            'path': path,
                            'sha': ref,
                        },
                        headers=headers,
                        timeout=REQUEST_TIMEOUT)

    resp.raise_for_status()
    c = resp.json()[0]

    return {
        'sha': c['sha'],
        'commit': c['commit'],
        'file_path': path,
        'timestamp': provenance.parse_date(c['commit']['committer']['date']),
    }


def get_model(path, ref='master'):
    headers = {
        'Accept': GITHUB_RAW_MEDIATYPE,
        'Authorization': 'token ' + GITHUB_AUTH_TOKEN,
    }

    document_url = os.path.join(GITHUB_CONTENTS_URL, path)

    resp = requests.get(document_url,
                        params={'ref': ref},
                        headers=headers,
                        timeout=REQUEST_TIMEOUT)

    if resp.status_code == 404:
        return None

    resp.raise_for_status()

    # Wrap decoded bytes in file-like object.
    buff = io.StringIO(resp.text)

    return Parser(buff).parse()


class Resource():
    def __init__(self, model_name, file_paths):
        self.model_name = model_name
        self.file_paths = file_paths

    @property
    def current_file_path(self):
        "Returns the current path of the document in the repository."
        return self.file_paths[-1]

    def get_all_commits(self):
        "Gets all commits for all paths for this document."
        commits = []

        for path in self.file_paths:
            commits.extend(get_commits(path))

        return commits

    def _get_ref_path(self, ref):
        "Resolve the full ref and file path of the passed ref."
        for path in self.file_paths:
            for c in get_commits(path):
                if c['sha'].startswith(ref):
                    return c['sha'], path

    def _get_ref_path_at_time(self, ts):
        ref = None
        path = None

        for p in self.file_paths:
            for c in get_commits(p):
                if c['timestamp'] > ts:
                    return ref, path

                ref = c['sha']
                path = p

            path = None
            ref = None

        return ref, path

    def _request_ref_path(self):
        "Get the ref and path from request args."
        ref = request.args.get('ref')
        asof = request.args.get('asof')

        # Explicit ref provided.
        if ref:
            return self._get_ref_path(ref)

        # Time provided.
        if asof:
            ts = provenance.parse_date(asof)
            return self._get_ref_path_at_time(ts)

        # Default to the latest.
        return None, self.current_file_path

    def serve_document(self):
        "HTTP handler for the document."
        ref, path = self._request_ref_path()

        if not path:
            return 'Not found', 404

        try:
            model = get_model(path, ref=ref)
        except HTTPError as e:
            return str(e), 503

        if not model:
            return 'Not found', 404

        model['name'] = self.model_name

        try:
            commit = get_commit(path, ref=ref)
        except HTTPError as e:
            return str(e), 503

        content = json.dumps({
            'commit': {
                'sha': commit['sha'],
                'timestamp': commit['timestamp'],
                'date': commit['commit']['committer']['date'],
                'file_path': commit['file_path'],
            },
            'model': model,
        })

        resp = Response(content)

        resp.headers['Content-Type'] = 'application/json'

        return resp

    def serve_provenance(self):
        "HTTP handler for the provenance."
        ref, path = self._request_ref_path()

        if not path:
            return 'not found', 404

        try:
            model = get_model(path, ref=ref)
        except HTTPError as e:
            return str(e), 503

        if not model:
            return 'not found', 404

        model['name'] = self.model_name

        try:
            commit = get_commit(path, ref=ref)
        except HTTPError as e:
            return str(e), 503

        prov = provenance.generate(file_name=self.current_file_path,
                                   domain='pedsnet.etlconv',
                                   model=model,
                                   commit=commit)

        if request.accept_mimetypes.best == 'application/json; boundary=NL':
            content = ldjson(prov)
            content_type = 'application/json; boundary=NL'
        else:
            content = json.dumps(prov, default=json_defaults)
            content_type = 'application/json'

        resp = Response(content)
        resp.headers['content-type'] = content_type

        return resp

    def serve_commits(self):
        commits = []

        for path in self.file_paths:
            commits.extend([{
                'file_path': path,
                'sha': c['sha'],
                'date': c['commit']['committer']['date'],
            } for c in get_commits(path)])

        commits = dedupe_commits(commits)

        resp = Response(json.dumps(commits))
        resp.headers['content-type'] = 'application/json'

        return resp

    def serve_log(self):
        entities = []

        for path in self.file_paths:
            for commit in get_commits(path):
                try:
                    model = get_model(path, commit['sha'])
                    model['name'] = self.model_name
                except Exception:
                    continue

                # Likely a 404 because the file moved in the current commit.
                if model is None:
                    continue

                model['name'] = self.model_name

                entities.extend(provenance.generate(file_name=path,
                                                    domain='pedsnet.etlconv',
                                                    model=model,
                                                    commit=commit))

        resp = Response(json.dumps(entities, default=json_defaults))
        resp.headers['content-type'] = 'application/json'

        return resp


pedsnet = Resource(
        model_name='pedsnet',
        file_paths=(
            'PEDSnet/docs/PEDSnet_CDM_V1_ETL_Conventions.md',
            'PEDSnet/V1/docs/PEDSnet_CDM_V1_ETL_Conventions.md',
            'PEDSnet/V2/docs/Pedsnet_CDM_V2_OMOPV5_ETL_Conventions.md',
            'PEDSnet/docs/Pedsnet_CDM_ETL_Conventions.md',
        ))

i2b2 = Resource(
        model_name='i2b2',
        file_paths=(
            'i2b2/V2/docs/i2b2_pedsnet_v2_etl_conventions.md',
        ))


# Initialize the flask app and register the routes.
app = Flask(__name__)

app.add_url_rule('/pedsnet',
                 'pedsnet',
                 pedsnet.serve_document,
                 methods=['GET'])

app.add_url_rule('/pedsnet/commits',
                 'pedsnet_commits',
                 pedsnet.serve_commits,
                 methods=['GET'])

app.add_url_rule('/pedsnet/prov',
                 'pedsnet_prov',
                 pedsnet.serve_provenance,
                 methods=['GET'])

app.add_url_rule('/pedsnet/log',
                 'pedsnet_prov_log',
                 pedsnet.serve_log,
                 methods=['GET'])

app.add_url_rule('/i2b2',
                 'i2b2',
                 i2b2.serve_document,
                 methods=['GET'])

app.add_url_rule('/i2b2/commits',
                 'i2b2_commits',
                 i2b2.serve_commits,
                 methods=['GET'])

app.add_url_rule('/i2b2/prov',
                 'i2b2_prov',
                 i2b2.serve_provenance,
                 methods=['GET'])

app.add_url_rule('/i2b2/log',
                 'i2b2_prov_log',
                 i2b2.serve_log,
                 methods=['GET'])

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

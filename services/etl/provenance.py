"""
Generate extracts a set of entities from the model and corresponding
commit relevant to the ETL conventions document. The set of entities
consist of ETL conventions data for the model, tables, and fields as
well as provenance information including causual or associated events
and the people or systems that were involved or are responsible.

All information produced here is stateless since historical information
is not available.

The structure of an entity is as follows:

- `domain` - The top-level domain the entity should be written to.
- `ident` - The unique identity value of the entity. Entities that do
not (or should not) be referenced by other entities do require this
to be set.
- `labels` - A set of labels for the entity. Each label provides a
secondary index for the entity.
- `attrs` - A set of key-value pairs where the key is a identity value
and the value is a literal or identity value.

An identity value is a reference to another entity. A reference value
consists of a domain and name. A string encoded reference value is
`domain:name`, that is the two parts delimited by a colon. Alternately,
an ident can represented as an object with keys `domain` and `name`.
"""
from copy import deepcopy
from datetime import datetime
from version import __version__


SPEC_VERSION = '1.0.0'


DATETIME_FORMATS = (
    '%Y-%m-%dT%H:%M:%SZ',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d',
)


def parse_date(s):
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(s, fmt).timestamp()
        except ValueError:
            pass


def validate(message):
    if not message.get('timestamp'):
        raise ValueError('timestamp is required')

    if not message.get('spec'):
        raise ValueError('spec is required')

    if 'attrs' not in message:
        return message

    for key, value in message['attrs'].items():
        if not isinstance(key, str):
            raise TypeError('attribute keys must be strings')

        # Reference value.
        if isinstance(value, dict):
            for k, v in value.items():
                if k != 'domain' and k != 'ident':
                    raise KeyError('only domain and ident keys are allowed in '
                                   'reference values')

                if not isinstance(v, str):
                    raise TypeError('domain and ident values must be strings '
                                    'not {}'.format(type(v)))

        elif not isinstance(value, (bool, str, float, int)):
            raise TypeError('unknown type {}. supported types are bool, '
                            'str, float, and int'.format(type(value)))

    return message


class entity():
    def __init__(self, data, timestamp):
        self.timestamp = timestamp
        self.spec = data.get('spec', SPEC_VERSION)
        self.domain = data.get('domain')
        self.labels = set(data.get('labels', ()))
        self.attrs = data.get('attrs', {})

        self._ident = ''
        self._domain = self.domain

        if 'ident' in data:
            ident = data['ident'].split(':')

            if len(ident) == 2:
                self._ident, self._domain = ident
            else:
                self._ident = ident[0]

    def __getitem__(self, key):
        if key == 'ident':
            return self.ident

        if key.startswith('_'):
            raise KeyError

        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.attrs[key] = value

    @property
    def ident(self):
        return {
            'domain': self._domain,
            'ident': self._ident,
        }

    def json(self):
        return validate({
            'timestamp': self.timestamp,
            'spec': self.spec,
            'domain': self.domain,
            'ident': self.ident,
            'labels': tuple(self.labels),
            'attrs': self.attrs,
        })


def generate(file_name, domain, model, commit):
    # Timestamp of when the entity became available.
    timestamp = parse_date(commit['commit']['committer']['date'])

    entities = []

    # Committer and author may be the same person, but entities
    # are deduped downstream.
    committer = commit['commit']['committer']

    committer = entity({
        'domain': domain,
        'ident': committer['email'],
        'labels': ['person', 'agent'],
        'attrs': committer,
    }, timestamp)

    author = commit['commit']['author']

    author = entity({
        'domain': domain,
        'ident': author['email'],
        'labels': ['person', 'agent'],
        'attrs': author,
    }, timestamp)

    # The git commit that corresponds to the current state. The metadata
    # of the commit does not technically need to be copied here. A consumer
    # of this data could lookup the commit and fetch the data manually.
    commit = entity({
        'domain': domain,
        'ident': commit['sha'],
        'labels': ['commit'],
        'attrs': {
            'sha': commit['sha'],
            'url': commit['commit']['url'],
            'message': commit['commit']['message'],
            'commit_time': commit['commit']['committer']['date'],
            'author_time': commit['commit']['author']['date'],
            'committer': committer.ident,
            'author': committer.ident,
        }
    }, timestamp)

    # The source file containing the data the model, tables, and fields
    # were extracted from.
    source_file = entity({
        'domain': domain,
        'ident': file_name,
        'labels': ['file'],
        'attrs': {
            'path': file_name,
            'commit': commit.ident,
        }
    }, timestamp)

    service = entity({
        'domain': domain,
        'ident': 'pedsnet/etlconv',
        'labels': ['agent', 'service'],
        'attrs': {
            'name': 'PEDSnet ETL Conventions Service',
            'version': __version__,
        },
    }, timestamp)

    # Append the entities.
    entities.append(service)
    entities.append(source_file)
    entities.append(commit)
    entities.append(author)
    entities.append(committer)

    # The base event is copied for each entity. This event describes
    # the relationship between the relationship between the Git commit
    # and the entity.
    base_event = {
        'domain': domain,
        'labels': ['event'],
        'attrs': {
            'event': 'extracted from file',
            'file': source_file.ident,
            'service': service.ident,
            'entity': {},
        }
    }

    model = dict(model)
    tables = model.pop('tables')

    model = entity({
        'domain': domain,
        'ident': model['name'],
        'labels': ['model'],
        'attrs': model,
    }, timestamp)

    entities.append(model)

    event = entity(deepcopy(base_event), timestamp)
    event['attrs']['entity'] = model.ident
    entities.append(event)

    for table_name, attrs in tables.items():
        # Copy to mutate the dict.
        attrs = dict(attrs)
        fields = attrs.pop('fields')

        attrs['name'] = table_name
        attrs['model'] = model.ident

        table_id = table_name

        table = entity({
            'domain': domain,
            'ident': table_id,
            'labels': ['table'],
            'attrs': attrs,
        }, timestamp)

        entities.append(table)

        event = entity(deepcopy(base_event), timestamp)
        event['attrs']['entity'] = table.ident
        entities.append(event)

        for field_name, attrs in fields.items():
            attrs = dict(attrs)

            field_id = '{}.{}'.format(table_name, field_name)

            attrs['table'] = table.ident
            attrs['model'] = model.ident
            attrs['name'] = field_name

            field = entity({
                'domain': domain,
                'ident': field_id,
                'labels': ['field'],
                'attrs': attrs,
            }, timestamp)

            entities.append(field)

            event = entity(deepcopy(base_event), timestamp)
            event['attrs']['entity'] = field.ident
            entities.append(event)

    return entities

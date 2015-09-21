import os

__version_info__ = {
    'major': 0,
    'minor': 2,
    'patch': 0,
    'release': 'beta',
    'sha': os.environ.get('GIT_SHA'),
}


def get_version(short=False):
    assert __version_info__['release'] in ('alpha', 'beta', 'final')

    vers = ['%(major)i.%(minor)i.%(patch)i' % __version_info__, ]

    if __version_info__['release'] != 'final' and not short:
        __version_info__['lvlchar'] = __version_info__['release'][0]

        vers.append('%(lvlchar)s' % __version_info__)

        if __version_info__['sha']:
            vers.append('+%(sha)s' % __version_info__)

    return ''.join(vers)


__version__ = get_version()

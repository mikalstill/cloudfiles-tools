#!/usr/bin/python

# Return an instantiated directory object depending on the URL given for the
# store type.

import sys

import local
import remote


def get_directory(container, url):
    if url.startswith('file://'):
        # Container should be none for local filesystem
        return local.LocalDirectory(url[len('file://'):])

    elif url.startswith('dfw://'):
        return remote.RemoteDirectory(container, url[len('dfw://'):])

    print 'Unknown container URL scheme: %s' % url
    sys.exit(1)

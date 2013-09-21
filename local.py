#!/usr/bin/python

# Methods to handle local files


import hashlib
import os


class LocalContainer(object):
    def __init__(self, name):
        self.name = name
        self.path = self.name.replace('file://', '')

    def get_directory(self, path):
        return LocalDirectory(self.path, path)


class LocalDirectory(object):
    def __init__(self, parent, path):
        if not path:
            self.path = parent
        else:
            self.path = os.path.join(parent, path)

    def listdir(self):
        for ent in sorted(os.listdir(self.path), reverse=True):
            yield ent

    def get_file(self, path):
        fullpath = os.path.join(self.path, path)
        return LocalFile(fullpath)


class LocalFile(object):
    def __init__(self, path):
        self.path = path
        self.cache = {}

    def checksum(self):
        if 'checksum' in self.cache:
            return self.cache['checksum']

        h = hashlib.sha512()
        with open(self.path, 'r') as f:
            h.update(f.read())
        self.cache['checksum'] = h.hexdigest()
        return self.cache['checksum']

    def size(self):
        if 'size' in self.cache:
            return self.cache['size']

        self.cache['size'] = os.path.getsize(self.path)
        return self.cache['size']

    def isdir(self):
        return os.path.isdir(self.path)

    def exists(self):
        return os.path.exists(self.path)

    def get_path(self):
        return self.path

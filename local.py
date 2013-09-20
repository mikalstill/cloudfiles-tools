#!/usr/bin/python

# Methods to handle local files


import hashlib
import os


class LocalDirectory(object):
    def __init__(self, path):
        self.path = path

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

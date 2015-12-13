#!/usr/bin/python

# Methods to handle local files


import datetime
import hashlib
import os
import random
import shutil


class LocalContainer(object):
    def __init__(self, name):
        self.name = name
        self.path = self.name.replace('file://', '')
        self.region = 'local'

    def get_directory(self, path):
        return LocalDirectory(self.region, self.path, path)


class LocalDirectory(object):
    def __init__(self, region, parent, path):
        self.region = region
        if not path:
            self.path = parent
        else:
            self.path = os.path.join(parent, path)

    def listdir(self):
        ents = os.listdir(self.path)
        random.shuffle(ents)
        for ent in ents:
            yield ent

    def get_file(self, path):
        fullpath = os.path.join(self.path, path)
        return LocalFile(self.region, fullpath)

    def update_shalist(self, path, checksum):
        pass

    def write_shalist(self):
        pass


class LocalFile(object):
    def __init__(self, region, path):
        self.region = region
        self.path = path
        self.cache = {}

    def checksum(self):
        if 'checksum' in self.cache:
            return self.cache['checksum']

        h = hashlib.sha512()
        with open(self.path, 'r') as f:
            d = f.read(1024 * 1204)
            while d:
                h.update(d)
                d = f.read(1024 * 1024)
        self.cache['checksum'] = h.hexdigest()
        return self.cache['checksum']

    def size(self):
        if 'size' in self.cache:
            return self.cache['size']

        self.cache['size'] = os.path.getsize(self.path)
        return self.cache['size']

    def store(self, local_file):
        d = os.path.dirname(self.path)
        if not os.path.exists(d):
            os.makedirs(d)
        print '%s Renaming %s to %s' %(datetime.datetime.now(), local_file, self.path)
        shutil.copy(local_file, self.path)

    def isdir(self):
        return os.path.isdir(self.path)

    def islink(self):
        return os.path.islink(self.path)

    def exists(self):
        return os.path.exists(self.path)

    def get_path(self):
        return self.path

    def store(self, path):
        parentdir = '/'.join(self.path.split('/')[:-1])
        if not os.path.exists(parentdir):
            os.makedirs(parentdir)
        shutil.copy(path, self.path)

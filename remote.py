#!/usr/bin/python

# Methods to handle remote files


import datetime
import hashlib
import json
import os

import pyrax

import utility


def remote_filename(filename):
    return filename.replace('/', '~')


def path_join(a, b):
    if a and b:
        return os.path.join(a, b)
    if a:
        return a
    return b


class RemoteContainer(object):
    def __init__(self, name):
        region, name = name.split('://')
        self.basename = os.path.basename(name)

        pyrax.set_setting('identity_type', 'rackspace')
        with open(os.path.expanduser('~/.cloudfiles'), 'r') as f:
            self.conf = json.loads(f.read())
            pyrax.set_credentials(self.conf['access_key'],
                                  self.conf['secret_key'],
                                  region=region)

        self.conn = pyrax.connect_to_cloudfiles(region=region.upper())
        self.container_name = remote_filename(name)
        self.container = self.conn.create_container(self.container_name)
        for i in range(3):
            try:
                self.container.log_retention(True)
                break
            except:
                pass

        for info in self.conn.list_containers_info():
            if info['name'] == self.container_name:
                remote_total = info['bytes']
                print ('%s Remote store contains %s in %d objects'
                       %(datetime.datetime.now(),
                         utility.DisplayFriendlySize(remote_total),
                         info['count']))

    def get_directory(self, path):
        return RemoteDirectory(self.container, path_join(self.basename, path))


class RemoteDirectory(object):
    def __init__(self, container, path):
        self.container = container
        self.path = path
        self.shalist = {}
        self.remote_files = {}

        if not self.path:
            self.shalist_path = '.shalist'
            prefix = None
        else:
            self.shalist_path = remote_filename(os.path.join(self.path,
                                                             '.shalist'))
            prefix = remote_filename(self.path)

        print '%s Fetching shalist %s' %(datetime.datetime.now(),
                                         self.shalist_path)
        for i in range(3):
            try:
                self.shalist = json.loads(container.get_object(
                        remote_filename(self.shalist_path)).fetch())
                break
            except:
                pass

        print '%s Finding existing remote files' % datetime.datetime.now()
        try:
            marker = None
            while True:
                results = self.container.get_objects(prefix=prefix,
                                                     marker=marker)
                print ('%s ... %d results, marker %s'
                       %(datetime.datetime.now(), len(results), marker))
                if not results:
                    break

                for f in results:
                    marker = f.name
                    if f.name.endswith('.sha512'):
                        pass
                    elif f.name.endswith('.shalist'):
                        pass
                    else:
                        self.remote_files[f.name.replace('~', '/')] = True

        except pyrax.exceptions.NoSuchObject:
            pass

        print '%s Found %d existing files' %(datetime.datetime.now(),
                                             len(self.remote_files))

    def listdir(self):
        for ent in self.shalist:
            yield ent

    def get_file(self, path):
        fullpath = path_join(self.path, path)
        r = RemoteFile(self.container, self.shalist, self.remote_files,
                       self.path, fullpath)
        if fullpath in self.shalist:
             r.cache['checksum'] = self.shalist[fullpath]
        return r


class RemoteFile(object):
    def __init__(self, container, shalist, remote_files, container_path, path):
        self.container = container
        self.shalist = shalist
        self.remote_files = remote_files
        self.path = path
        self.container_path = container_path
        self.cache = {}

    def checksum(self):
        if 'checksum' in self.cache:
            print '%s Checksum  %s' %(datetime.datetime.now(),
                                      self.cache['checksum'])
            return self.cache['checksum']

        write_remote_checksum = False

        try:
            self.cache['checksum'] = self.container.get_object(
                remote_filename(self.path + '.sha512')).fetch()
            self.container.delete_object(
                remote_filename(self.path + '.sha512'))
            write_remote_checksum = True
            print ('%s Found old style checksum for %s'
                   %(datetime.datetime.now(), self.path))
        except:
            print ('%s Missing checksum for %s'
                   %(datetime.datetime.now(), self.path))
            h = hashlib.sha512()
            h.update(self.container.get_object(
                    remote_filename(self.path)).fetch())
            self.cache['checksum'] = h.hexdigest()
            write_remote_checksum = True

        if write_remote_checksum:
            self.write_checksum(self.cache['checksum'])
        
        return self.cache['checksum']

    def size(self):
        if 'size' in self.cache:
            return self.cache['size']

        self.cache['size'] = os.path.getsize(self.path)
        return self.cache['size']

    def isdir(self):
        return os.path.isdir(self.path)

    def exists(self):
        return self.path in self.remote_files

    def write_checksum(self, checksum):
        self.shalist[self.path] = checksum
        self.cache['checksum'] = checksum

        shafile = remote_filename(os.path.join(self.container_path,
                                               '.shalist'))
        print '%s Updating  %s' %(datetime.datetime.now(), shafile)

        for i in range(3):
            try:
                try:
                    obj = self.container.delete_object(shafile)
                except:
                    pass
                obj = self.container.store_object(
                    shafile, json.dumps(self.shalist, sort_keys=True, indent=4))
                break
            except Exception as e:
                print ('%s Upload    FAILED TO UPLOAD CHECKSUM (%s)'
                       %(datetime.datetime.now(), e))

    def get_path(self):
        return self.path

    def store(self, local_path):
        # Uploads sometimes timeout. Retry three times.
        for i in range(3):
            try:
                obj = self.container.upload_file(
                    local_path, obj_name=remote_filename(local_path))
                break
            except Exception as e:
                print '%s Upload    FAILED (%s)' %(datetime.datetime.now(), e)

            

#!/usr/bin/python

# Methods to handle remote files


import datetime
import hashlib
import json
import os
import progressbar
import sys
import tempfile
import urllib2

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
        self.region, name = name.split('://')
        self.basename = os.path.basename(name)

        pyrax.set_setting('identity_type', 'rackspace')
        with open(os.path.expanduser('~/.cloudfiles'), 'r') as f:
            self.conf = json.loads(f.read())
            pyrax.set_credentials(self.conf['access_key'],
                                  self.conf['secret_key'],
                                  region=self.region)

        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())

        if self.region == 'dfw':
            self.container_name = remote_filename(name)
        else:
            self.container_name = remote_filename('%s/%s' %(self.region, name))
        container = conn.create_container(self.container_name)

        for i in range(3):
            try:
                container.log_retention(True)
                break
            except:
                pass

        for info in conn.list_containers_info():
            if info['name'] == self.container_name:
                remote_total = info['bytes']
                print ('%s Remote store %s contains %s in %d objects'
                       %(datetime.datetime.now(), self.region,
                         utility.DisplayFriendlySize(remote_total),
                         info['count']))

    def get_directory(self, path):
        return RemoteDirectory(self.region, self.container_name,
                               path_join(self.basename, path))


class RemoteDirectory(object):
    def __init__(self, region, container_name, path):
        self.region = region
        self.container_name = container_name
        self.path = path
        self.shalist = {}
        self.remote_files = {}

        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        if not self.path:
            self.shalist_path = '.shalist'
            prefix = None
        else:
            self.shalist_path = remote_filename(os.path.join(self.path,
                                                             '.shalist'))
            prefix = remote_filename(self.path)

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
                results = container.get_objects(prefix=prefix, marker=marker)
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

        print ('%s Found %d existing files in %s'
               %(datetime.datetime.now(), len(self.remote_files), self.region))

    def listdir(self):
        for ent in self.shalist.keys():
            if ent in self.remote_files:
                yield ent

        # Directories don't appear in shalists
        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        dirs = {}
        marker = None
        prefix = remote_filename(self.path) + '~'
        while True:
            results = container.get_objects(prefix=prefix, marker=marker)
            if not results:
                break
            for f in results:
                marker = f.name
                if f.name.endswith('.shalist'):
                    subdir = f.name[len(prefix):]
                    if subdir and subdir != '.shalist':
                        dirs[subdir.split('~')[0]] = True

        for d in dirs:
            yield d

    def get_file(self, path):
        fullpath = path_join(self.path, path)
        r = RemoteFile(self.region, self.container_name, self.shalist,
                       self.remote_files, self.path, fullpath)
        if fullpath in self.shalist:
             r.cache['checksum'] = self.shalist[fullpath]
        return r


class RemoteFile(object):
    def __init__(self, region, container_name, shalist, remote_files,
                 container_path, path):
        self.region = region
        self.container_name = container_name
        self.shalist = shalist
        self.remote_files = remote_files
        self.path = path
        self.container_path = container_path
        self.cache = {}

    def checksum(self):
        if 'checksum' in self.cache:
            return self.cache['checksum']

        write_remote_checksum = False

        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        try:
            self.cache['checksum'] = container.get_object(
                remote_filename(self.path + '.sha512')).fetch()
            container.delete_object(
                remote_filename(self.path + '.sha512'))
            write_remote_checksum = True
            print ('%s Found old style checksum for %s'
                   %(datetime.datetime.now(), self.path))
        except:
            print ('%s Missing checksum for %s'
                   %(datetime.datetime.now(), self.path))
            local_file = self.fetch()
            h = hashlib.sha512()
            with open(local_file) as f:
                d = f.read(1024 * 1204)
                while d:
                    h.update(d)
            os.remove(local_file)

            self.cache['checksum'] = h.hexdigest()
            write_remote_checksum = True

        if write_remote_checksum:
            self.write_checksum(self.cache['checksum'])

        return self.cache['checksum']

    def size(self):
        if 'size' in self.cache:
            return self.cache['size']

        print ('%s Querying the size of %s in %s'
               %(datetime.datetime.now(), self.path, self.region))
        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)
        obj = container.get_object(remote_filename(self.path))
        self.cache['size'] = obj.total_bytes
        return self.cache['size']

    def isdir(self):
        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        prefix = remote_filename(self.path) + '~'
        results = container.get_objects(prefix=prefix)
        if not results:
            return False
        return True

    def islink(self):
        return False

    def exists(self):
        return self.path in self.remote_files

    def write_checksum(self, checksum):
        self.shalist[self.path] = checksum
        self.cache['checksum'] = checksum

        shafile = remote_filename(os.path.join(self.container_path, '.shalist'))
        print '%s Updating  %s with %s' %(datetime.datetime.now(), shafile,
                                          self.path)

        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        for i in range(3):
            try:
                try:
                    obj = container.delete_object(shafile)
                except:
                    pass
                obj = container.store_object(
                    shafile, json.dumps(self.shalist, sort_keys=True, indent=4))
                break
            except Exception as e:
                print ('%s Upload    FAILED TO UPLOAD CHECKSUM (%s)'
                       %(datetime.datetime.now(), e))

    def get_path(self):
        return self.path

    def store(self, local_path):
        # Uploads sometimes timeout. Retry three times.
        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        for i in range(3):
            try:
                obj = container.upload_file(
                    local_path, obj_name=remote_filename(self.path))
                break
            except Exception as e:
                print '%s Upload    FAILED (%s)' %(datetime.datetime.now(), e)

    def fetch(self):
        conn = pyrax.connect_to_cloudfiles(region=self.region.upper())
        container = conn.create_container(self.container_name)

        (local_fd, local_file) = tempfile.mkstemp()
        os.close(local_fd)

        # Small files we just fetch
        if self.size() < 100 * 1024 * 1024:
            with open(local_file, 'w') as f:
                f.write(container.get_object(
                     remote_filename(self.path)).fetch())
        else:
            url = container.get_object(remote_filename(self.path)).get_temp_url(
                3600)
            url = url.replace(' ', '%20')
            print '%s Fetch URL is %s' %(datetime.datetime.now(), url)

            widgets = ['Fetching: ', ' ', progressbar.Percentage(), ' ',
                       progressbar.Bar(marker=progressbar.RotatingMarker()),
                       ' ', progressbar.ETA(), ' ',
                       progressbar.FileTransferSpeed()]
            pbar = progressbar.ProgressBar(widgets=widgets,
                                           maxval=self.size()).start()

            r = urllib2.urlopen(url)
            count = 0
            try:
                with open(local_file, 'w') as f:
                    d = r.read(409600)
                    count += len(d)
                    while d:
                        f.write(d)
                        d = r.read(14096)
                        count += len(d)
                        pbar.update(count)

            finally:
                pbar.finish()
                print '%s Fetch finished' % datetime.datetime.now()
                r.close()

        return local_file

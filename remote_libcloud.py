# Methods to handle remote files via libcloud

import contextlib
import datetime
import hashlib
import json
import os
import sys
import tempfile
import urllib2

import libcloud
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

import utility


def remote_filename(filename):
    return filename.replace('/', '~')


def get_driver_helper(provider_name):
    if provider_name == 'cloudfiles':
        return get_driver(Provider.CLOUDFILES_US)
    elif provider_name == 's3':
        return get_driver(Provider.S3)
    else:
        print 'Unknown provider %s!' % provider_name
        sys.exit(1)


@contextlib.contextmanager
def data_in_file(d):
    try:
        (local_fd, local_file) = tempfile.mkstemp()
        os.write(local_fd, d)
        os.close(local_fd)
        yield local_file
    finally:
        os.remove(local_file)


class RemoteContainer(object):
    def __init__(self, name):
        self.provider_name, filename = name.split('@')
        self.region, name = filename.split('://')
        self.basename = os.path.basename(name)
        self.provider = get_driver_helper(self.provider_name)

        with open(os.path.expanduser('~/.cloudfiles'), 'r') as f:
            self.conf = json.loads(f.read())
            self.conn = self.provider(
                self.conf[self.provider_name]['access_key'],
                self.conf[self.provider_name]['secret_key'],
                ex_force_service_region=self.region)
            self.storage_class = self.conf[self.provider_name].get(
                'storage_class', 'standard')

        if self.provider_name == 's3':
            # s3 container names must be valid DNS names
            self.container_name = \
                ('%s.%s' %(self.region, name)).replace('/', '.')
        elif self.region == 'dfw':
            self.container_name = remote_filename(name)
        else:
            self.container_name = remote_filename('%s/%s' %(self.region, name))

        # Force container creation
        self.get_container()

    def get_directory(self, path):
        return RemoteDirectory(self, utility.path_join(self.basename, path))

    # Helper methods for this driver (not part of the base interface)
    def get_connection(self):
        return self.conn

    def get_name(self):
        return self.container_name

    def get_container(self):
        try:
            return self.conn.get_container(self.container_name)
        except libcloud.storage.types.ContainerDoesNotExistError:
            return self.conn.create_container(self.container_name)

    def get_class(self):
        return self.storage_class

    def get_provider(self):
        return self.provider_name


class RemoteDirectory(object):
    def __init__(self, parent_container, path):
        # NOTE(mikal): this is needed so that a hack in the layer above works
        self.region = parent_container.region

        self.parent_container = parent_container
        self.path = path
        self.shalist = {}
        self.remote_files = {}

        self.conn = self.parent_container.get_connection()

        if not self.path:
            self.shalist_path = '.shalist'
            prefix = None
        else:
            self.shalist_path = remote_filename(os.path.join(self.path,
                                                             '.shalist'))
            prefix = remote_filename(self.path)

        try:
            obj = self.parent_container.get_container().get_object(
                remote_filename(self.shalist_path))
            (local_fd, local_file) = tempfile.mkstemp()
            os.close(local_fd)
            obj.download(local_file, overwrite_existing=True,
                         delete_on_failure=True)

            with open(local_file) as f:
                self.shalist = json.loads(f.read())

            os.remove(local_file)
        except libcloud.storage.types.ObjectDoesNotExistError:
            pass

        for obj in self.conn.iterate_container_objects(
            self.parent_container.get_container(), ex_prefix=prefix):
            if obj.name.endswith('.sha512'):
                pass
            elif obj.name.endswith('.shalist'):
                pass
            else:
                self.remote_files[obj.name.replace('~', '/')] = True

    def listdir(self):
        for ent in self.shalist.keys():
            if ent in self.remote_files:
                yield ent

        # Directories don't appear in shalists
        prefix = remote_filename(self.path + '/')
        for obj in self.conn.iterate_container_objects(
            self.parent_container.get_container(), ex_prefix=prefix):
            if obj.name.endswith('.shalist'):
                subdir = obj.name[len(prefix):]
                if subdir and subdir != '.shalist':
                    yield subdir.split('~')[0]

    def get_file(self, path):
        fullpath = utility.path_join(self.path, path)
        r = RemoteFile(self.parent_container, self, fullpath)
        if fullpath in self.shalist:
             r.cache['checksum'] = self.shalist[fullpath]
        return r

    def get_path(self):
        return self.path

    def update_shalist(self, path, checksum):
        self.shalist[path] = checksum

    def write_shalist(self):
        shafile = remote_filename(os.path.join(self.path, '.shalist'))
        print '%s Updating  %s with %s' %(datetime.datetime.now(), shafile,
                                          self.path)

        with data_in_file(json.dumps(self.shalist,
                                     sort_keys=True, indent=4)) as f:
            self.conn.upload_object(f, self.parent_container.get_container(),
                                    shafile)

    def file_exists(self, path):
        return path in self.remote_files


class RemoteFile(object):
    def __init__(self, parent_container, parent_directory, path):
        # NOTE(mikal): this is needed so that a hack in the layer above works
        self.region = parent_container.region

        self.parent_container = parent_container
        self.parent_directory = parent_directory
        self.path = path
        self.conn = self.parent_container.get_connection()
        self.cache = {}

    def checksum(self):
        if 'checksum' in self.cache:
            return self.cache['checksum']

        print ('%s Computing checksum for %s'
               %(datetime.datetime.now(), self.path))
        local_file = self.fetch()
        h = hashlib.sha512()
        with open(local_file) as f:
            d = f.read(1024 * 1204)
            while d:
                h.update(d)
                d = f.read(1024 * 1024)
        os.remove(local_file)

        self.cache['checksum'] = h.hexdigest()
        self.write_checksum(self.cache['checksum'])
        return self.cache['checksum']

    def size(self):
        if 'size' in self.cache:
            return self.cache['size']

        print ('%s Querying the size of %s'
               %(datetime.datetime.now(), self.path))
        obj = self.parent_container.get_container().get_object(
            remote_filename(self.path))
        self.cache['size'] = obj.size
        return self.cache['size']

    def isdir(self):
        prefix = remote_filename(self.path) + '~'
        for obj in self.conn.iterate_container_objects(
            self.parent_container.get_container(), ex_prefix=prefix):
            return True
        return False

    def islink(self):
        return False

    def exists(self):
        return self.parent_directory.file_exists(self.path)

    def write_checksum(self, checksum):
        shafile = remote_filename(os.path.join(
            self.parent_directory.get_path(), '.shalist'))

        shalist = {}
        try:
            obj = self.parent_container.get_container().get_object(
                    shafile)
            (local_fd, local_file) = tempfile.mkstemp()
            os.close(local_fd)
            obj.download(local_file, overwrite_existing=True,
                         delete_on_failure=True)

            with open(local_file) as f:
                shalist = json.loads(f.read())

            os.remove(local_file)
        except libcloud.storage.types.ObjectDoesNotExistError:
            pass
        
        shalist[self.path] = checksum
        self.cache['checksum'] = checksum

        print '%s Updating  %s with %s' %(datetime.datetime.now(), shafile,
                                          self.path)

        with data_in_file(json.dumps(shalist, sort_keys=True, indent=4)) as f:
            self.conn.upload_object(f, self.parent_container.get_container(),
                                    shafile)

    def get_path(self):
        return self.path

    def store(self, local_path):
        kwargs = {}
        if self.parent_container.get_provider() == 's3':
            kwargs['ex_storage_class'] = self.parent_container.get_class()
        
        self.conn.upload_object(
            local_path,
            self.parent_container.get_container(),
            remote_filename(self.path),
            **kwargs)

    def fetch(self):
        obj = self.parent_container.get_container().get_object(
            remote_filename(self.path))

        (local_fd, local_file) = tempfile.mkstemp()
        os.close(local_fd)

        self.conn.download_object(obj, local_file, overwrite_existing=True,
                                  delete_on_failure=True)

        return local_file

#!/usr/bin/env python

# $1 is the path to the directory to sync
# $2 is the name of the remote container to use

import datetime
import json
import os
import sys
import time

import sys
sys.path.append('/data/src/stillhq_public/trunk/python/')

import utility

import local
import remote

uploaded = 0
destination_total = 0


class NoSuchException(Exception):
    pass


def upload_directory(source_container, destination_container, path):
    global uploaded
    global destination_total

    print '%s Syncing %s' %(datetime.datetime.now(), path)
    source_dir = source_container.get_directory(path)
    destination_dir = destination_container.get_directory(path)

    queued_shas = {}
    for ent in source_dir.listdir():
        # NOTE(mikal): this is a work around to handle the historial way
        # in which the directory name appears in both the container name and
        # path inside the container for remote stores. It was easier than
        # rewriting the contents of the remote stores.
        if source_dir.region != 'local':
            ent = '/'.join(os.path.split(ent)[1:])

        fullpath = remote.path_join(path, ent)
        source_file = source_dir.get_file(ent)

        if source_file.isdir():
            upload_directory(source_container, destination_container, fullpath)

        elif source_file.islink():
            pass

        elif source_file.get_path().endswith('.sha512'):
            pass

        elif source_file.get_path().endswith('.shalist'):
            pass

        elif source_file.get_path().endswith('~'):
            pass

        else:
            destination_file = destination_dir.get_file(ent)
            print '%s Consider  %s' %(datetime.datetime.now(),
                                      source_file.get_path())
            if destination_file.exists():
                if destination_file.checksum() != source_file.checksum():
                    print ('%s Checksum for %s does not match! (%s vs %s)'
                           %(datetime.datetime.now(), source_file.get_path(),
                             source_file.checksum(),
                             destination_file.checksum()))
                else:
                    continue

            try:
                local_file = source_file.get_path()
                local_cleanup = False
                if not source_file.region == 'local':
                    print ('%s Fetching the file from remote location'
                           % datetime.datetime.now())
                    local_cleanup = True
                    local_file = source_file.fetch()

                print ('%s Uploading %s (%s)'
                       %(datetime.datetime.now(), source_file.get_path(),
                         utility.DisplayFriendlySize(source_file.size())))
                start_time = time.time()
                destination_file.store(local_file)
                queued_shas[source_file.checksum()] = destination_file

                if len(queued_shas) > 20 or source_file.size() > 1024 * 1024:
                    for sha in queued_shas:
                        queued_shas[sha].write_checksum(sha)

                if local_cleanup:
                    os.remove(local_file)

                print ('%s Uploaded  %s (%s)'
                       %(datetime.datetime.now(), source_file.get_path(),
                         utility.DisplayFriendlySize(source_file.size())))
                uploaded += source_file.size()
                destination_total += source_file.size()
                elapsed = time.time() - start_time
                print '%s Total     %s' %(datetime.datetime.now(),
                                          utility.DisplayFriendlySize(uploaded))
                print ('%s           %s per second'
                       %(datetime.datetime.now(),
                         utility.DisplayFriendlySize(int(source_file.size() /
                                                         elapsed))))
                print ('%s Stored    %s'
                       %(datetime.datetime.now(),
                         utility.DisplayFriendlySize(destination_total)))
            except NoSuchException, e:
                sys.stderr.write('%s Sync failed for %s: %s'
                                 %(datetime.datetime.now(),
                                   source_file.get_path(),
                                   e))

            for sha in queued_shas:
                xqueued_shas[sha].write_checksum(sha)

            if uploaded > 10 * 1024 * 1024 * 1024:
                print '%s Maximum upload reached' % datetime.datetime.now()
                sys.exit(0)


def get_container(url):
    if url.startswith('file://'):
        return local.LocalContainer(url)
    else:
        return remote.RemoteContainer(url)


if __name__ == '__main__':
    source_container = get_container(sys.argv[1])
    destination_container = get_container(sys.argv[2])
    upload_directory(source_container, destination_container, None)
    print '%s Finished' % datetime.datetime.now()
    print '%s Total     %s' %(datetime.datetime.now(),
                              utility.DisplayFriendlySize(uploaded))


#!/usr/bin/python

# $1 is the path to the directory to sync
# $2 is the name of the remote container to use

import cloudfiles
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
remote_total = 0


def upload_directory(container, path):
    global uploaded
    global remote_total

    print '%s Syncing %s' %(datetime.datetime.now(), path)
    local_dir = local.LocalDirectory(path)
    remote_dir = remote.RemoteDirectory(container, path)

    for ent in local_dir.listdir():
        fullpath = os.path.join(path, ent)
        local_file = local_dir.get_file(ent)

        if local_file.isdir():
            upload_directory(container, fullpath)
        elif not fullpath.endswith('.sha512'):
            remote_file = remote_dir.get_file(ent)
            print '%s Consider  %s' %(datetime.datetime.now(), fullpath)
            if remote_file.exists():
                print '%s Exists    %s' %(datetime.datetime.now(),
                                          fullpath)
                if remote_file.checksum() != local_file.checksum():
                    print ('%s Checksum for %s does not match! (%s vs %s)'
                           %(datetime.datetime.now(), fullpath,
                             local_file.checksum(), remote_file.checksum()))
                else:
                    continue

            print ('%s Uploading %s (%s)'
                   %(datetime.datetime.now(), fullpath,
                     utility.DisplayFriendlySize(local_file.size())))
            start_time = time.time()

            # Uploads sometimes timeout. Retry three times.
            for i in range(3):
                try:
                    obj = container.create_object(
                        remote.remote_filename(fullpath))
                    obj.load_from_filename(fullpath)
                    break
                except:
                    print '%s Upload    FAILED' % datetime.datetime.now()

            remote_file.write_checksum(local_file.checksum())

            print ('%s Uploaded  %s (%s)'
                   %(datetime.datetime.now(), fullpath,
                     utility.DisplayFriendlySize(local_file.size())))
            uploaded += local_file.size()
            remote_total += local_file.size()
            elapsed = time.time() - start_time
            print '%s Total     %s' %(datetime.datetime.now(),
                                      utility.DisplayFriendlySize(uploaded))
            print ('%s           %s per second'
                   %(datetime.datetime.now(),
                     utility.DisplayFriendlySize(int(local_file.size() /
                                                     elapsed))))
            print ('%s Stored    %s'
                   %(datetime.datetime.now(),
                     utility.DisplayFriendlySize(remote_total)))

            if uploaded > 2 * 1024 * 1024 * 1024:
                print '%s Maximum upload reached' % datetime.datetime.now()
                sys.exit(0)

if __name__ == '__main__':
    with open(os.path.expanduser('~/.cloudfiles'), 'r') as f:
        conf = json.loads(f.read())
    conn = cloudfiles.get_connection(conf['access_key'],
                                     conf['secret_key'],
                                     timeout=30)

    container_name = remote.remote_filename(sys.argv[2])
    container = conn.create_container(container_name)
    for i in range(3):
        try:
            container.log_retention(True)
            break
        except:
            pass

    for info in conn.list_containers_info():
        if info['name'] == container_name:
            remote_total = info['bytes']
            print ('%s Remote store contains %s in %d objects'
                   %(datetime.datetime.now(),
                     utility.DisplayFriendlySize(remote_total), info['count']))

    upload_directory(container, sys.argv[1])
    print '%s Finished' % datetime.datetime.now()

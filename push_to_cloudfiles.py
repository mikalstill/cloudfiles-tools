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
import stores

uploaded = 0
remote_total = 0


def upload_directory(local_container, remote_container, path):
    global uploaded
    global remote_total

    print '%s Syncing %s' %(datetime.datetime.now(), path)
    local_dir = local_container.get_directory(path)
    remote_dir = remote_container.get_directory(path)

    for ent in local_dir.listdir():
        if path:
            fullpath = os.path.join(path, ent)
        else:
            fullpath = ent

        local_file = local_dir.get_file(ent)

        if local_file.isdir():
            upload_directory(local_container, remote_container, fullpath)

        elif not local_file.get_path().endswith('.sha512'):
            remote_file = remote_dir.get_file(ent)
            print '%s Consider  %s' %(datetime.datetime.now(),
                                      local_file.get_path())
            if remote_file.exists():
                print '%s Exists    %s' %(datetime.datetime.now(),
                                          local_file.get_path())
                if remote_file.checksum() != local_file.checksum():
                    print ('%s Checksum for %s does not match! (%s vs %s)'
                           %(datetime.datetime.now(), local_file.get_path(),
                             local_file.checksum(), remote_file.checksum()))
                else:
                    continue

            print ('%s Uploading %s (%s)'
                   %(datetime.datetime.now(), local_file.get_path(),
                     utility.DisplayFriendlySize(local_file.size())))
            start_time = time.time()

            # Uploads sometimes timeout. Retry three times.
            for i in range(3):
                try:
                    obj = remote_container.create_object(local_file.get_path())
                    obj.load_from_filename(local_file.get_path())
                    break
                except Exception as e:
                    print '%s Upload    FAILED (%s)' %(datetime.datetime.now(),
                                                       e)

            remote_file.write_checksum(local_file.checksum())

            print ('%s Uploaded  %s (%s)'
                   %(datetime.datetime.now(), local_file.get_path(),
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
    local_container = local.LocalContainer(sys.argv[1])
    remote_container = remote.RemoteContainer(sys.argv[2])
    upload_directory(local_container, remote_container, None)
    print '%s Finished' % datetime.datetime.now()

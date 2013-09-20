#!/usr/bin/python

# $1 is the path to the directory to sync
# $2 is the name of the remote container to use

import cloudfiles
import datetime
import hashlib
import json
import os
import sys
import time

import sys
sys.path.append('/data/src/stillhq_public/trunk/python/')

import utility


remote = {}
uploaded = 0
remote_total = 0


def remote_filename(filename):
    return filename.replace('/', '~')


def upload_directory(container, path):
    global remote
    global start_time
    global uploaded
    global remote_total

    print '%s Fetching shalist for %s' %(datetime.datetime.now(), path)
    shalist = {}
    for i in range(3):
        try:
            shalist = json.loads(container.get_object(
                    remote_filename(os.path.join(path, '.shalist'))).read())
            break
        except:
            pass

    print '%s Syncing %s' %(datetime.datetime.now(), path)
    for ent in sorted(os.listdir(path), reverse=True):
        fullpath = os.path.join(path, ent)
        if os.path.isdir(fullpath):
            upload_directory(container, fullpath)
        elif not fullpath.endswith('.sha512'):
            h = hashlib.sha512()
            with open(fullpath, 'r') as f:
                h.update(f.read())
            local_checksum = h.hexdigest()
            local_size = os.path.getsize(fullpath)

            if fullpath in remote:
                write_remote_checksum = False
                if fullpath in shalist:
                    remote_checksum = shalist[fullpath]
                else:
                    try:
                        remote_checksum = container.get_object(
                            remote_filename(fullpath + '.sha512')).read()
                        container.delete_object(
                            remote_filename(fullpath + '.sha512'))
                        write_remote_checksum = True
                        print ('%s Found old style checksum for %s'
                               %(datetime.datetime.now(), fullpath))
                    except:
                        print ('%s Missing checksum for %s'
                               %(datetime.datetime.now(), fullpath))
                        h = hashlib.sha512()
                        h.update(container.get_object(
                                remote_filename(fullpath)).read())
                        remote_checksum = h.hexdigest()
                        write_remote_checksum = True

                if write_remote_checksum:
                    print ('%s Updating checksum for %s to new style'
                           %(datetime.datetime.now(), fullpath))
                    try:
                        shalist[fullpath] = remote_checksum
                        shafile = remote_filename(
                            os.path.join(path, '.shalist'))
                        try:
                            obj = container.delete_object(shafile)
                            time.sleep(1)
                        except:
                            pass
                        obj = container.create_object(shafile)
                        obj.write(json.dumps(shalist, sort_keys=True,
                                             indent=4))
                        container.delete_object(
                            remote_filename(fullpath + '.sha512'))
                        break
                    except:
                        print ('%s Upload    FAILED TO UPLOAD CHECKSUM'
                               % datetime.datetime.now())

                if remote_checksum != local_checksum:
                    print ('%s Checksum for %s does not match! (%s vs %s)'
                               %(datetime.datetime.now(), fullpath,
                                 local_checksum, remote_checksum))
                else:
                    continue

            print ('%s Uploading %s (%s)'
                   %(datetime.datetime.now(), fullpath,
                     utility.DisplayFriendlySize(os.path.getsize(fullpath))))
            start_time = time.time()

            # Uploads sometimes timeout. Retry three times.
            for i in range(3):
                try:
                    obj = container.create_object(remote_filename(fullpath))
                    obj.load_from_filename(fullpath)
                    break
                except:
                    print '%s Upload    FAILED' % datetime.datetime.now()

            shalist[fullpath] = local_checksum
            for i in range(3):
                try:
                    shafile = remote_filename(os.path.join(path, '.shalist'))
                    try:
                        obj = container.delete_object(shafile)
                    except:
                        pass
                    obj = container.create_object(shafile)
                    obj.write(json.dumps(shalist, sort_keys=True, indent=4))
                    break
                except:
                    print ('%s Upload    FAILED TO UPLOAD CHECKSUM'
                           % datetime.datetime.now())

            print ('%s Uploaded  %s (%s)'
                   %(datetime.datetime.now(), fullpath,
                     utility.DisplayFriendlySize(local_size)))
            uploaded += local_size
            remote_total += local_size
            elapsed = time.time() - start_time
            print '%s Total     %s' %(datetime.datetime.now(),
                                      utility.DisplayFriendlySize(uploaded))
            print ('%s           %s per second'
                   %(datetime.datetime.now(),
                     utility.DisplayFriendlySize(int(local_size / elapsed))))
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

    container_name = remote_filename(sys.argv[2])
    container = conn.create_container(container_name)
    for i in range(3):
        try:
            container.log_retention(True)
            break
        except:
            pass

    containers_info = conn.list_containers_info()
    for info in containers_info:
        if info['name'] == container_name:
            remote_total = info['bytes']
            print ('%s Remote store contains %s in %d objects'
                   %(datetime.datetime.now(),
                     utility.DisplayFriendlySize(remote_total), info['count']))
                                          

    print '%s Finding existing remote files' % datetime.datetime.now()
    try:
        for f in container.get_objects():
            if f.name.endswith('.sha512'):
                pass
            elif f.name.endswith('.shalist'):
                pass
            else:
                remote[f.name.replace('~', '/')] = True

    except cloudfiles.errors.NoSuchObject:
        pass

    print '%s Found %d existing files' %(datetime.datetime.now(), len(remote))

    upload_directory(container, sys.argv[1])
    print '%s Finished' % datetime.datetime.now()

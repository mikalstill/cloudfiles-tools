#!/usr/bin/python

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


if __name__ == '__main__':
    with open(os.path.expanduser('~/.cloudfiles'), 'r') as f:
        conf = json.loads(f.read())
        conn = cloudfiles.get_connection(conf['access_key'],
                                         conf['secret_key'],
                                         timeout=30)

    container = conn.get_container('molokai~data~picture')
    for f in container.get_objects():
        print f.name
        try:
            container.delete_object(f.name)
        except:
            pass
    conn.delete_container('molokai~data~picture')

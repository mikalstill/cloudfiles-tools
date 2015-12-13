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
                                         timeout=30,
                                         region='ord')

    print conn.get_containers()
    container_name = 'ord~molokai~data~pictures'
    container = conn.get_container(container_name)
    for f in container.get_objects():
        print f.name
        try:
            container.delete_object(f.name)
        except:
            pass
    conn.delete_container(container_name)

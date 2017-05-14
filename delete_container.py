#!/usr/bin/python

import json
import os
import sys

import libcloud
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver

import utility


def get_driver_helper(provider_name):
    if provider_name == 'cloudfiles':
        return get_driver(Provider.CLOUDFILES_US)
    elif provider_name == 's3':
        return get_driver(Provider.S3)
    else:
        print 'Unknown provider %s!' % provider_name
        sys.exit(1)


if __name__ == '__main__':
    provider_name = 'cloudfiles'
    region = sys.argv[1]
    container_name = sys.argv[2]

    provider = get_driver_helper(provider_name)

    with open(os.path.expanduser('~/.cloudfiles'), 'r') as f:
        conf = json.loads(f.read())
        conn = provider(
            conf[provider_name]['access_key'],
            conf[provider_name]['secret_key'],
            ex_force_service_region=region)
        storage_class = conf[provider_name].get(
            'storage_class', 'standard')

    container = conn.get_container(container_name)
    for f in container.list_objects():
        print f.name
        container.delete_object(f)
    conn.delete_container(container)

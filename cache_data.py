"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import re
import sys
import json
import boto.utils
import time
import traceback
from random import random
from multiprocessing import Pool
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
from nlp_client.caching import useCaching
from nlp_client.services import *

useCaching(writeOnly=True)

workers = int(sys.argv[1])
services = sys.argv[2] if len(sys.argv) > 2 else 'services-config.json'

services = json.loads(open(services).read())['services']
BUCKET = S3Connection().get_bucket('nlp-data')

while True:
    processes = []
    keys = [key.name for key in BUCKET.list(prefix='data_events/') if key.name.endswith('gz')]
    while len(keys) > 0:
        while len(processes) < workers:
            processes += [Popen(['/usr/bin/python', 'cache_data_child.py', keys.pop()])]
        processes = filter(lambda x: x.poll() is None, processes)
        time.sleep(0.25)
    time.sleep(15)

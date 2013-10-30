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
from nlp_client.caching import useCaching
from nlp_client.services import *

workers = int(sys.argv[1])
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

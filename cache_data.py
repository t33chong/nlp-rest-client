"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import sys
import time
import re
from boto import connect_s3
from subprocess import Popen
from nlp_client.caching import useCaching
from nlp_client.services import *

workers = int(sys.argv[1])
BUCKET = connect_s3().get_bucket('nlp-data')

counter = 0
while True:
    processes = []
    keys = [key.name for key in BUCKET.list(prefix='data_events/') if re.sub(r'/?data_events/?', '', key.name) is not '']
    while len(keys) > 0:
        while len(processes) < workers:
            if counter > 0:
                print 'done'
                sys.exit()
            processes += [Popen(['/usr/bin/python', 'cache_data_child.py', keys.pop()])]
            counter += 1
        processes = filter(lambda x: x.poll() is None, processes)
        time.sleep(0.25)
    time.sleep(15)

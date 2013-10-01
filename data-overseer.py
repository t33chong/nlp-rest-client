"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import sys
import json
from time import sleep
from subprocess import Popen
from boto.s3.connection import S3Connection
from boto.s3.key import Key

workers = int(sys.argv[1])

SERVICES = 'services-config.json'
CREDENTIALS = 'aws.json'

key = json.loads(open(CREDENTIALS).read())['key']
secret = json.loads(open(CREDENTIALS).read())['secret']
bucket = S3Connection(key, secret).get_bucket('nlp-data')
k = Key(bucket)

eventfiles = [eventfile.name for eventfile in bucket.get_all_keys(prefix='data_events/')]

processes = []

for i in range(0, len(eventfiles), workers):
    print 'i', i
    current = eventfiles[i:i+workers]
    print 'working on:', current
    processes = map(lambda x: Popen('python data-harvester.py %s' % x, shell=True), current)
    while len(processes) > 0:
        print 'processes:', processes
        processes = filter(lambda x: x.poll() is None, processes)
        sleep(5)
    ## delete all event files in current batch when complete - uncomment
    #for eventfile in current:
    #    k.key = eventfile
    #    k.delete()

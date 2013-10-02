"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import sys
import json
from time import sleep
from subprocess import Popen
from multiprocessing import Pool
from boto.s3.connection import S3Connection
from boto.s3.key import Key

workers = int(sys.argv[1])

SERVICES = 'services-config.json'

bucket = S3Connection().get_bucket('nlp-data')

eventfiles = [eventfile.name for eventfile in bucket.get_all_keys(prefix='data_events/')]

def call_harvester(eventfile):
    print 'STARTING EVENT FILE %s' % eventfile
    harvester = Popen('python data-harvester.py %s' % eventfile, shell=True)
    harvester.wait()
    print 'EVENT FILE %s COMPLETE' % eventfile
    k = Key(bucket)
    k.key = eventfile
    k.delete()

pool = Pool(processes=workers)
pool.map(call_harvester, eventfiles)

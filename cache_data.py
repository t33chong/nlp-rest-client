"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import re
import sys
import json
from time import sleep
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

bucket = S3Connection().get_bucket('nlp-data')

eventfiles = [eventfile.name for eventfile in bucket.get_all_keys(prefix='data_events/')]

def call_services(eventfile):
    print 'STARTING EVENT FILE %s' % eventfile
    k = Key(bucket)
    k.key = eventfile
    try:
        for filename in k.get_contents_as_string().split('\n'):
            try:
                match = re.search('([0-9]+)/([0-9]+)', filename)
                doc_id = '%s_%s' % (match.group(1), match.group(2))
                print 'Calling services on', eventfile, doc_id
                for service in services:
                    # dynamically call the specified service
                    try:
                        call = getattr(sys.modules[__name__], service)().get(doc_id)
                    except:
                        print '%s: Could not call %s on %s!' % (eventfile, service, doc_id)
            except AttributeError:
                print '%s: line "%s" is an unexpected format.' % (eventfile, filename)
        print 'EVENT FILE %s COMPLETE' % eventfile
        k.delete()
    except S3ResponseError:
        print 'EVENT FILE %s NOT FOUND!' % eventfile

pool = Pool(processes=workers)
pool.map(call_services, eventfiles)

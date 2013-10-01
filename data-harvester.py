"""
Calls a specified set of services on each pageid/XML file listed in the given input file in order to warm the cache.
"""

import re
import sys
import json
from time import sleep
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from nlp_client.services import *
from nlp_client.caching import useCaching

eventfile = sys.argv[1]
services = sys.argv[2] if len(sys.argv) > 2 else 'services-config.json'

useCaching(writeOnly=True)

bucket = S3Connection().get_bucket('nlp-data')

services = json.loads(open(services).read())['services']

k = Key(bucket)
k.key = eventfile

for filename in k.get_contents_as_string().split('\n'):
    try:
        match = re.search('([0-9]+)/([0-9]+)', filename)
        doc_id = '%s_%s' % (match.group(1), match.group(2))
        print 'Calling services on', eventfile, doc_id
        for service in services:
            #print service
            # dynamically call the specified service
            try:
                call = getattr(sys.modules[__name__], service)().get(doc_id)
                #print call
            except:
                print '%s: Could not call %s on %s!' % (eventfile, service, doc_id)
    except AttributeError:
        print '%s: line "%s" is an unexpected format.' % (eventfile, filename)

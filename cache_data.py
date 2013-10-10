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

def call_services(keyname):
    global BUCKET

    key = BUCKET.get_key(keyname)
    if key is None:
        return

    SIG = "%s_%s_%s" % (boto.utils.get_instance_metadata()['local-hostname'], str(time.time()), str(int(random()*100)))
    eventfile = 'data_processing/'+SIG
    try:
        key.copy('nlp-data', eventfile)
        key.delete()
    except S3ResponseError:
        print 'EVENT FILE %s NOT FOUND!' % eventfile
        return
    except KeyboardInterrupt:
        sys.exit()

    print 'STARTING EVENT FILE %s' % eventfile
    k = Key(BUCKET)
    k.key = eventfile

    def processFile(filename):
        try:
            match = re.search('([0-9]+)/([0-9]+)', filename)
            doc_id = '%s_%s' % (match.group(1), match.group(2))
            for service in services:
                # dynamically call the specified service
                try:
                    call = getattr(sys.modules[__name__], service)().get(doc_id)
                except KeyboardInterrupt:
                    sys.exit()
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()

                    print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

                    print 'Could not call %s on %s!' % (service, doc_id)
        except AttributeError:
            print 'Unexpected format: %s:' % (filename)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        except KeyboardInterrupt:
            sys.exit()

    map(processFile, k.get_contents_as_string().split(u'\n'))

    print 'EVENT FILE %s COMPLETE' % eventfile
    k.delete()



pool = Pool(processes=workers)

while True:
    pool.map(call_services, [key.name for key in BUCKET.list(prefix='data_events/') if key.name.endswith('gz')])
    time.sleep(30)

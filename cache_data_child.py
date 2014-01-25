from nlp_services.syntax import AllNounPhrasesService, AllVerbPhrasesService, HeadsService
from nlp_services.discourse.entities import CoreferenceCountsService, EntityCountsService
from nlp_services.discourse.sentiment import DocumentSentimentService, DocumentEntitySentimentService, WpDocumentEntitySentimentService
from nlp_services.caching import use_caching
from multiprocessing import Pool
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
import traceback
import boto
import sys
import re
import json
import time
import random

BUCKET = boto.connect_s3().get_bucket('nlp-data')

service_file = sys.argv[2] if len(sys.argv) > 2 else 'services-config.json'
SERVICES = json.loads(open(service_file).read())['services']

use_caching(per_service_cache=dict([(service+'.get', {'write_only': True}) for service in SERVICES]))

def process_file(filename):
    if filename.strip() == '':
        return  # newline at end of file
    global SERVICES
    match = re.search('([0-9]+)/([0-9]+)', filename)
    if match is None:
        print "No match for %s" % filename
        return

    doc_id = '%s_%s' % (match.group(1), match.group(2))
    for service in SERVICES:
        try:
            getattr(sys.modules[__name__], service)().get(doc_id)
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            print 'Could not call %s on %s!' % (service, doc_id)
            print traceback.format_exc()


def call_services(keyname):
    global BUCKET

    key = BUCKET.get_key(keyname)
    if key is None:
        return

    eventfile = "data_processing/%s_%s_%s" % (boto.utils.get_instance_metadata()['local-hostname'], str(time.time()), str(int(random.randint(0, 100))))
    try:
        key.copy('nlp-data', eventfile)
        key.delete()
    except S3ResponseError as e:
        print e
        print 'EVENT FILE %s NOT FOUND!' % eventfile
        return
    except KeyboardInterrupt:
        sys.exit()

    print 'STARTING EVENT FILE %s' % eventfile
    k = Key(BUCKET)
    k.key = eventfile

    print k.key
    map(process_file, k.get_contents_as_string().split('\n'))
            
    print 'EVENT FILE %s COMPLETE' % eventfile
    k.delete()


call_services(sys.argv[1])

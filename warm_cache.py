import logging
import os
import sys
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from nlp_services.syntax import WikiToPageHeadsService
from nlp_services.discourse.entities import WikiPageToEntitiesService
from nlp_services.caching import use_caching
from time import sleep

use_caching()

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
fh = logging.FileHandler('log_warm_cache.log')
fh.setLevel(logging.INFO)
log.addHandler(fh)
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
log.addHandler(sh)

bucket = S3Connection().get_bucket('nlp-data')

def add_files():
    keys = filter(lambda x: x.key.endswith('.txt'), bucket.list('text_events/'))
    for key in keys:
        old_key_name = key.key
        print 'Found key %s' % old_key_name
        try:
            new_key_name = os.path.join('text_processing', os.path.basename(old_key_name))
            key.copy(bucket, new_key_name)
            key.delete()
        except S3ResponseError:
            continue
        newkey = Key(bucket)
        newkey.key = new_key_name
        wid = newkey.get_contents_as_string()
        newkey.delete()
        return wid
    return False

def call_services(wid):
    log.info('Calling services on %s' % wid)
    try:
        log.debug('%s HEADS: %s' % (wid, WikiToPageHeadsService().get(wid)))
        log.debug('%s ENTITIES: %s' % (wid, WikiPageToEntitiesService().get(wid)))
    except:
        log.error('ERROR: %s\n%s' % (wid, traceback.format_exc()))
    else:
        log.info('Successfully completed %s' % wid)

while True:
    wid = add_files()
    if not wid:
        sys.exit(0)
    call_services(wid)
    sleep(10)

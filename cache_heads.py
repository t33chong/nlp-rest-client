import logging
import os
import sys
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from nlp_services.syntax import HeadsService
from nlp_services.caching import use_caching

use_caching()

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
fh = logging.FileHandler('log_cache_heads.log')
fh.setLevel(logging.INFO)
log.addHandler(fh)
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
log.addHandler(sh)

bucket = S3Connection().get_bucket('nlp-data')

def get_from_queue():
    keys = filter(lambda x: x.key.endswith('.txt'), bucket.list('heads_events/'))
    for key in keys:
        old_key_name = key.key
        print 'Found key %s' % old_key_name
        try:
            new_key_name = os.path.join('heads_processing', os.path.basename(old_key_name))
            key.copy(bucket, new_key_name)
            key.delete()
        except S3ResponseError:
            continue
        newkey = Key(bucket)
        newkey.key = new_key_name
        ids = newkey.get_contents_as_string().split('\n')
        newkey.delete()
        return ids
    return False

def cache_heads(ids):
    for id_ in ids:
        log.info('Caching heads for %s' % id_)
        try:
            log.debug('%s HEADS: %s' % (id_, HeadsService().get(id_)))
        except:
            log.error('ERROR: %s\n%s' % (id_, traceback.format_exc()))

while True:
    ids = get_from_queue()
    if not ids:
        sys.exit(0)
    cache_heads(ids)

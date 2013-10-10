"""
Calls AllTitlesService and RedirectsService on a set of wikis, writes resulting data to S3.
"""

import os
import sys
import json
import gzip
import logging
from multiprocessing import Pool
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from WikiaSolr import QueryIterator
from nlp_client import title_confirmation
from nlp_client.services import AllTitlesService, RedirectsService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('dump_titles.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)


title_confirmation.USE_S3 = False

titles_dir = '/data/titles/'
redirects_dir = '/data/redirects/'

bucket = S3Connection().get_bucket('nlp-data')
k = Key(bucket)

def call_titles(doc):
    try:
        logger.debug('Calling AllTitlesService on %s' % doc)
        titles = json.dumps(AllTitlesService().get(doc), ensure_ascii=False)
        titles_file = titles_dir + doc + '.gz'
        g = gzip.GzipFile(titles_file, 'w')
        g.write(titles)
        g.close()
        k.key = 'article_titles/%s' % os.path.basename(titles_file)
        k.set_contents_from_filename(titles_file)
        os.remove(titles_file)
    except:
        logger.error('TITLES SERVICE FAILED ON %s!' % doc)
        #raise

def call_redirects(doc):
    try:
        logger.debug('Calling RedirectsService on %s' % doc)
        redirects = json.dumps(RedirectsService().get(doc), ensure_ascii=False)
        redirects_file = redirects_dir + doc + '.gz'
        g = gzip.GzipFile(redirects_file, 'w')
        g.write(redirects)
        g.close()
        k.key = 'article_redirects/%s' % os.path.basename(redirects_file)
        k.set_contents_from_filename(redirects_file)
        os.remove(redirects_file)
    except:
        logger.error('REDIRECTS SERVICE FAILED ON %s!' % doc)
        #raise

def call_both(doc):
    logger.info('Calling both services on %s' % doc)
    call_titles(doc)
    call_redirects(doc)

def gen_docs():
    """Generator that yields the wiki id of each result in the QueryIterator"""
    qi = QueryIterator('http://search-s10:8983/solr/xwiki/', {'query': 'lang_s:en', 'fields': 'id', 'sort': 'wam_i desc', 'start': 1000})
    for doc in qi:
        logger.debug('yielding %s...' % doc['id'])
        yield doc['id']

docs = gen_docs()

pool = Pool(processes=7)
pool.map(call_both, docs)

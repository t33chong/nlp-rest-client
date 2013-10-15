"""
Iterates over query queue files and writes text from queries specified in the
query queue files.
"""

import os
import sys
import json
import uuid
import socket
import shutil
import logging
import tarfile
import requests
from optparse import OptionParser
from multiprocessing import Process, Queue
from subprocess import Popen
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from utils import clean_list, ensure_dir_exists
from WikiaSolr import QueryIterator

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('write_text.log')
fh.setLevel(logging.ERROR)
logger.addHandler(fh)

# Load default values from config file
nlp_config = json.loads(open('nlp-config.json').read())[socket.gethostname()]
workers = nlp_config['workers']

# Allow user to configure options
parser = OptionParser()
parser.add_option("-n", "--workers", dest="workers", action="store", default=workers,
                  help="Specify the number of worker processes to open")
parser.add_option("-l", "--local", dest="local", action="store_true", default=False,
                  help="Specify whether to store text files locally instead of on S3")
(options, args) = parser.parse_args()

WORKERS = options.workers
LOCAL = options.local

EVENT_DIR = ensure_dir_exists('/data/events/')
TEMP_EVENT_DIR = ensure_dir_exists('/data/temp_events/')
TEXT_DIR = ensure_dir_exists('/data/text/')
TEMP_TEXT_DIR = ensure_dir_exists('/data/temp_text/')

def write_text(event_file):
    """Takes event file as input, writes text from all queries containted in
    event file to TEXT_DIR, and returns the number of documents written"""
    doc_count = 0
    for line in open(event_file):
        query = line.strip()
        logger.info('Writing query: "%s"' % query)
        qi = QueryIterator('http://search-s11.prod.wikia.net:8983/solr/main/', {'query': query, 'fields': 'id,wid,html_en,indexed', 'sort': 'id asc'})
        for doc in qi:
            # sanitize and write text
            text = '\n'.join(clean_list(doc.get('html_en', '')))
            localpath = os.path.join(TEXT_DIR, doc['id'])
            logger.debug('writing to %' % localpath)
            with open(localpath, 'w') as f:
                f.write(text)
            doc_count += 1
    return doc_count

def write_worker(event_queue, count_queue):
    """Takes queue of event files as input, returns count of docs written as
    output"""
    try:
        for event_file in iter(event_queue.get, None):
            temp_event_file = os.path.join(TEMP_EVENT_DIR, os.path.basename(event_file))
            shutil.move(event_file, temp_event_file)
            doc_count = write_text(temp_event_file)
            os.remove(event_file)
            count_queue.put(doc_count)
    except Exception as e:
        logger.error('%s: %s' % (type(e).__name__, e))
        count_queue.put(0)

if __name__ == '__main__':
    # List of query queue files to iterate over
    event_files = [os.path.join(EVENT_DIR, event_file) for event_file in os.listdir(EVENT_DIR)]
    event_queue = Queue()
    count_queue = Queue()

    for event_file in event_files:
        event_queue.put(event_file)

    write_workers = [Process(target=write_worker, args=(event_queue, count_queue)) for n in range(WORKERS)]

    for write_worker in write_workers:
        write_worker.start()

    batch_count = 0
    for count in iter(count_queue.get, None):
        batch_count += count
        if batch_count > 500:
            text_files = [(os.path.join(TEXT_DIR, filename), os.path.getmtime(filename)) for filename in os.listdir(TEXT_DIR)]
            text_files.sort(key=lambda x: x[1])
            tempid = str(uuid.uuid4())
            tempdir = os.path.join(TEXT_TEMP_DIR, tempid)
            for text_file in text_files[:500]:
                shutil.move(text_file, os.path.join(tempdir, os.path.basename(text_file)))
            Popen('python %s %s %i' % ('tar_batch.py', tempdir, int(LOCAL)), shell=True)
            batch_count -= 500

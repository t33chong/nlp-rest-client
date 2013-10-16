"""
Iterates over query queue files and writes text from queries specified in the
query queue files.
"""

# TODO:
# Ensure that final batch of <BATCHSIZE goes through

import os
import sys
import json
import uuid
import socket
import shutil
import logging
import tarfile
import requests
import traceback
from time import sleep
from optparse import OptionParser
from multiprocessing import Process, Queue
from multiprocessing.process import current_process
from subprocess import Popen
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from utils import clean_list, ensure_dir_exists
from WikiaSolr import QueryIterator

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
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
parser.add_option("-b", "--batchsize", dest="batchsize", action="store", default=500,
                  help="Specify the number of worker processes to open")
parser.add_option("-l", "--local", dest="local", action="store_true", default=False,
                  help="Specify whether to store text files locally instead of on S3")
(options, args) = parser.parse_args()

WORKERS = options.workers
BATCHSIZE = options.batchsize
LOCAL = options.local

EVENT_DIR = ensure_dir_exists('/data/events/')
TEMP_EVENT_DIR = ensure_dir_exists('/data/temp_events/')
TEXT_DIR = ensure_dir_exists('/data/text/')
TEMP_TEXT_DIR = ensure_dir_exists('/data/temp_text/')

if not LOCAL:
    bucket = S3Connection().get_bucket('nlp-data')

def write_text(event_file):
    """Takes event file as input, writes text from all queries contained in
    event file to TEXT_DIR, and returns a list of documents written"""
    text_files = []
    for line in open(event_file):
        query = line.strip()
        logger.info('Writing query from %s: "%s"' % (current_process(), query))
        qi = QueryIterator('http://search-s11.prod.wikia.net:8983/solr/main/', {'query': query, 'fields': 'id,wid,html_en,indexed', 'sort': 'id asc'})
        for doc in qi:
            # sanitize and write text
            text = '\n'.join(clean_list(doc.get('html_en', '')))
            localpath = os.path.join(TEXT_DIR, doc['id'])
            logger.debug('Writing text from %s to %s' % (doc['id'], localpath))
            with open(localpath, 'w') as f:
                f.write(text)
            text_files.append(localpath)
    return text_files

def write_worker(event_queue, text_file_queue):
    """Takes queue of event files, moves each file to TEMP_EVENT_DIR, calls
    write_text() on the aforementioned file, and adds the returned list of
    written files to a queue of text files"""
    for event_file in iter(event_queue.get, None):
        try:
            temp_event_file = os.path.join(TEMP_EVENT_DIR, os.path.basename(event_file))
            shutil.move(event_file, temp_event_file)
            text_files = write_text(temp_event_file)
            for text_file in text_files:
                text_file_queue.put(text_file)
            os.remove(temp_event_file)
        except:
            logger.error(traceback.print_exc())

def tar_batch(text_batch_dir):
    """Takes a text batch directory, tars it, and optionally uploads the
    resulting archive to S3"""
    tarball_path = text_batch_dir + '.tgz'
    logger.debug('Archiving batch to %s' % tarball_path)
    tarball = tarfile.open(tarball_path, 'w:gz')
    tarball.add(text_batch_dir, '.')
    tarball.close()
    wids = list(set([docid.split('_')[0] for docid in os.listdir(text_batch_dir)]))
    logger.debug('%s contains wids: %s' % (tarball_path, ','.join(wids)))
    shutil.rmtree(text_batch_dir)
    if not LOCAL:
        logger.debug('Uploading %s to S3' % os.path.basename(tarball_path))
        k = Key(bucket)
        k.key = 'text_events/%s' % os.path.basename(tarball_path)
        k.set_contents_from_filename(tarball_path)
        os.remove(tarball_path)
        for wid in wids:
            requests.post('http://nlp-s1:5000/wiki/%i' % wid)
        return 'Tarball %s uploaded to S3' % os.path.basename(tarball_path)
    with open('/data/tarball_key.txt', 'a') as f:
        f.write('%s\t%s\n' % (tarball_path, ','.join(wids)))
    return 'Tarball stored locally at %s' % tarball_path

def tar_worker(text_file_queue, tar_result_queue):
    """Takes queue of text files, moves them to individual subdirectories of
    TEMP_TEXT_DIR in batches of BATCHSIZE, calls tar_batch() on the aforementioned
    subdirectories, and adds the returned result to a queue"""
    files_in_batch = 0
    temp_text_batch_dir = ensure_dir_exists(os.path.join(TEMP_TEXT_DIR, str(uuid.uuid4())))
    for text_file in iter(text_file_queue.get, None):
        try:
            shutil.move(text_file, os.path.join(temp_text_batch_dir, os.path.basename(text_file)))
            files_in_batch += 1
            # Call tar_batch() if batch meets size requirement
            if files_in_batch >= BATCHSIZE:
                logger.info('Writing batch of size %i to %s' % (files_in_batch, temp_text_batch_dir))
                tar_result = tar_batch(temp_text_batch_dir)
                tar_result_queue.put(tar_result)
                files_in_batch = 0
                temp_text_batch_dir = ensure_dir_exists(os.path.join(TEMP_TEXT_DIR, str(uuid.uuid4())))
        except:
            logger.error(traceback.print_exc())
    # Handle final batch in the queue in case it doesn't meet BATCHSIZE requirement
    try:
        logger.info('Writing batch of size %i to %s' % (files_in_batch, temp_text_batch_dir))
        tar_result = tar_batch(temp_text_batch_dir)
        tar_result_queue.put(tar_result)
    except:
        logger.error(traceback.print_exc())

if __name__ == '__main__':
    while True:
        # List of query queue files to iterate over
        event_files = [os.path.join(EVENT_DIR, event_file) for event_file in os.listdir(EVENT_DIR)]
        logging.info('Iterating over %i event files...' % len(event_files))

        # If there are no query queue files present, wait and retry
        if not event_files:
            logger.info('No event files found, sleeping for 60 seconds...')
            sleep(60)
            continue

        event_queue = Queue()
        text_file_queue = Queue()

        for event_file in event_files:
            event_queue.put(event_file)

        write_workers = [Process(target=write_worker, args=(event_queue, text_file_queue)) for n in range(WORKERS)]

        for write_worker in write_workers:
            write_worker.start()

        tar_result_queue = Queue()

        tar_workers = [Process(target=tar_worker, args=(text_file_queue, tar_result_queue)) for n in range(WORKERS)]

        for tar_worker in tar_workers:
            tar_worker.start()

        for tar_result in iter(tar_result_queue.get, None):
            logger.info(tar_result)

        if not text_file_queue.empty():
            pass # TODO

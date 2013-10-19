import os
import shutil
import logging
import tarfile
import requests
import traceback
from time import sleep
from uuid import uuid4
from optparse import OptionParser
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from utils import ensure_dir_exists
from query_write import TEXT_DIR, TEMP_TEXT_DIR

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('query_tar.log')
fh.setLevel(logging.ERROR)
logger.addHandler(fh)

# Allow user to configure options
parser = OptionParser()
parser.add_option("-b", "--batchsize", dest="batchsize", action="store", default=500,
                  help="Specify the maximum number of files in a .tgz batch")
parser.add_option("-l", "--local", dest="local", nargs=0, action="store_true", default=False,
                  help="Specify whether to store text files locally instead of on S3")
(options, args) = parser.parse_args()

BATCHSIZE = options.batchsize
LOCAL = options.local

if not LOCAL:
    bucket = S3Connection().get_bucket('nlp-data')

def list_text_files():
    """Return a list of files in TEXT_DIR, sorted chronologically"""
    text_files = [(os.path.join(TEXT_DIR, filename), os.path.getmtime(os.path.join(TEXT_DIR, filename))) for filename in os.listdir(TEXT_DIR)]
    text_files.sort(key=lambda x: x[1])
    return text_files

# Set to run indefinitely
while True:

    #try:
    # Attempt to enforce minimum batch size, continue after 30 seconds if not
    if len(os.listdir(TEXT_DIR)) < BATCHSIZE:
        logger.warning('Current batch does not meet %i file minimum, waiting for 30 seconds...' % BATCHSIZE)
        sleep(30)
    text_files = list_text_files()

    # Move text files to temp directory
    text_batch_dir = ensure_dir_exists(os.path.join(TEMP_TEXT_DIR, str(uuid4())))
    files_in_batch = 0
    for text_file in text_files[:BATCHSIZE]:
        shutil.move(text_file[0], os.path.join(text_batch_dir, os.path.basename(text_file[0])))
        files_in_batch += 1
    logger.info('Moving batch of size %i to %s' % (files_in_batch, text_batch_dir))

    # Tar batch
    tarball_path = text_batch_dir + '.tgz'
    logger.info('Archiving batch to %s' % tarball_path)
    tarball = tarfile.open(tarball_path, 'w:gz')
    tarball.add(text_batch_dir, '.')
    tarball.close()

    # Get list of wiki ids represented in this batch, remove temp directory
    wids = list(set([docid.split('_')[0] for docid in os.listdir(text_batch_dir)]))
    logger.debug('%s contains wids: %s' % (tarball_path, ','.join(wids)))
    shutil.rmtree(text_batch_dir)

    # Optionally upload to S3
    if not LOCAL:
        logger.info('Uploading %s to S3' % os.path.basename(tarball_path))
        k = Key(bucket)
        k.key = 'text_events/%s' % os.path.basename(tarball_path)
        k.set_contents_from_filename(tarball_path)
        os.remove(tarball_path)

        # Send post request to start parser for these wiki ids
        for wid in wids:
            requests.post('http://nlp-s1:5000/wiki/%s' % wid)
    else:
        # Record represented wiki ids for future use
        with open('/data/tarball_key.txt', 'a') as f:
            f.write('%s\t%s\n' % (tarball_path, ','.join(wids)))
        logger.debug('Tarball stored locally at %s' % tarball_path)
    #except:
    #    logger.error(traceback.print_exc())

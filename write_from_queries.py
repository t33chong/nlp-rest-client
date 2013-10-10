"""
Iterates over query queue files and writes text from queries specified in the
query queue files.
"""

import os
import sys
import json
import socket
import shutil
import logging
import tarfile
import requests
from optparse import OptionParser
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

EVENT_DIR = '/data/events/'
PROC_DIR = '/data/processing/'
TEXT_DIR = '/data/text/'

# List of query queue files to iterate over
event_files = [os.path.join(EVENT_DIR, event_file) for event_file in os.listdir(EVENT_DIR)]

def write_text(event_file):
    for line in open(event_file):
        query = line.strip()
        

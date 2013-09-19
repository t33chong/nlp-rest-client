"""
Responsible for iterating over wids in the XML directory and writing entity data
collected from harvester subprocesses to file.
"""

import json, socket
from optparse import OptionParser
import sys
sys.path.append('..')
from WikiaSolr.overseer import WriteOverseer

nlp_config = json.loads(open('nlp-config.json').read())[socket.gethostname()]
workers = nlp_config['workers']

parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store", default=True,
                  help="Shows verbose output")
parser.add_option("-n", "--workers", dest="workers", action="store", default=workers,
                  help="Specifies the number of open worker processes")
parser.add_option("-a", "--aws", dest="aws", action="store", default=0,
                  help="Specify whether to write text files to Amazon S3; int as boolean")
parser.add_option("-q", "--qqdir", dest="qqdir", action="store", default="/data/events",
                  help="Path to directory containing query queue files")
parser.add_option("-p", "--processing", dest="processing", action="store", default="/data/processing",
                  help="Path to processing directory")

(options, args) = parser.parse_args()

WriteOverseer(vars(options)).oversee()

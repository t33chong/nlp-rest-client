"""
Responsible for handling the event stream - iterates over files in the data_events S3 bucket and calls a set of services on each pageid/XML file listed in order to warm the cache.
"""

import json
from optparse import OptionParser
import sys
from WikiaSolr.overseer import DataOverseer

parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store", default=True,
                  help="Shows verbose output")
parser.add_option("-n", "--workers", dest="workers", action="store", default=4,
                  help="Specifies the number of open worker processes")
parser.add_option("-s", "--services", dest="services", action="store", default='services-config.json',
                  help="Points to a JSON file containing the names of services to call")
parser.add_option("-c", "--credentials", dest="credentials", action="store", default='aws.json',
                  help="Points to a JSON file containing AWS credentials")

(options, args) = parser.parse_args()

DataOverseer(vars(options)).oversee()

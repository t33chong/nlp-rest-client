import requests
import json
import traceback
import sys
import os
import random
from boto import connect_s3
from boto.s3.prefix import Prefix
from multiprocessing import Pool
from nlp_client.services import TopEntitiesService, EntityDocumentCountsService, TopHeadsService
from nlp_client.caching import useCaching

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
useCaching(writeOnly=True)

def callServices(wid):
    try:
        TopEntitiesService().get(wid)
        EntityDocumentCountsService().get(wid)
        TopHeadsService().get(wid)
        print wid
        return 1
    except:
        print "Problem with", wid
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    

wids = [prefix.name.split('/')[-2] for prefix in connect_s3().get_bucket('nlp-data').list(prefix='xml/', delimiter='/') if isinstance(prefix, Prefix)]
# shuffled to improve coverage across a pool
random.shuffle(wids)
Pool(processes=3).map(callServices, wids)

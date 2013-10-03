import requests
import json
import traceback
import sys
from boto import connect_s3
from boto.s3.prefix import Prefix
from multiprocessing import Pool
from nlp_client.services import TopEntitiesService, EntityDocumentCountsService, TopHeadsService
from nlp_client.caching import useCaching

useCaching(writeOnly=True)

def callServices(wid):
    print "Working on", wid
    try:
        print wid, TopEntitiesService().nestedGet(wid)
        print wid, EntityDocumentCountsService().nestedGet(wid)
        print wid, TopHeadsService().nestedGet(wid)
        print "Finished with", wid
        return 1
    except:
        print "Problem with", wid
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    


"""
bucketList = connect_s3().bucket('nlp-data').list(prefix='xml/', delimiter='/')
pool = Pool(processes=4)
result = pool.map(sendToWiki, bucketList)
"""

while True:
    wids = [prefix.name.split('/')[-2] for prefix in connect_s3().get_bucket('nlp-data').list(prefix='xml/', delimiter='/') if isinstance(prefix, Prefix)]
    Pool(processes=3).map(callServices, wids)

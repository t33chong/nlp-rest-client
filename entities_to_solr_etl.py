"""
This script transfers top entity data from AWS to Solr
Because it's run on the Wikia side, we're only iterating over wikis with a warmed cache
"""

import requests
import json
from boto import connect_s3
from boto.s3.prefix import Prefix
from multiprocessing import Pool
from nlp_client.services import TopEntitiesService

topService = TopEntitiesService()

def sendToWiki(prefix):
    global topService
    prefix
    if isinstance(prefix, Prefix):
        return requests.post('http://search-s11:8983/solr/xwiki/update', 
                             json.dumps({'entities_txt':topService.nestedGet(prefix.name.split('/')[-2])})
                             headers={'Content-type':'application/json'}).content
    return None

bucketList = connect_s3().bucket('nlp-data').list(prefix='service_responses/', delimiter='/')

pool = Pool(processes=4)
result = pool.map(sendToWiki, bucketList)

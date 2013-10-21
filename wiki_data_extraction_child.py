import sys
import traceback
import os
from boto import connect_s3
from nlp_client.services import TopEntitiesService, EntityDocumentCountsService, TopHeadsService
from nlp_client.caching import useCaching

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
useCaching(writeOnly=True, services={'EntityCountsService.get': {'write_only': False}) # reuse entity counts for now


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


def process(x):
    if connect_s3().get_bucket('nlp-data').get_key('service_responses/%s/TopEntitiesService.get' % x) is not None:
        print x, "already processed"
    else:
        print "Calling services on", x
        callServices(x)


wid = sys.argv[1]
if len(sys.argv) > 2:
    process(wid)
else:
    callServices(wid)


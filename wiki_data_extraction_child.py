import sys
import traceback
import os
from boto import connect_s3
from nlp_client.services import TopEntitiesService, EntityDocumentCountsService, TopHeadsService, WpTopEntitiesService, WpEntityDocumentCountsService
from nlp_client.caching import useCaching

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
psc = {'TopEntitiesService.get': {'write_only': True}, 'EntityDocumentCountsService.get' : {'write_only': True}, 'TopHeadsService.get': {'write_only': True}, 'WikiEntitiesService.get': {'write_only': True}, 'WpTopEntitiesService.get': {'write_only': True}, 'WpEntityCountsService.get': {'write_only':True}, 'WpEntitiesService.get': {'write_only':True}, 'WpWikiEntitiesService.get': {'write_only':True, 'WpEntityDocumentCountsService': {'write_only': True}}}
useCaching(perServiceCaching=psc)

wid = sys.argv[1]
try:
    TopEntitiesService().get(wid)
    del psc['EntityDocumentCountsService.get']
    useCaching(perServiceCaching=psc)
    EntityDocumentCountsService().get(wid)
    TopHeadsService().get(wid)
    WpTopEntitiesService().get(wid)
    del psc['WpEntityCountsService.get']
    useCaching(perServiceCaching=psc)
    print WpEntityDocumentCountsService().get(wid)
    print wid
except:
    print "Problem with", wid
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

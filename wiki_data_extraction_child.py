import sys
import traceback
import os
import json
from boto import connect_s3
from nlp_services.caching import use_caching
from nlp_services.syntax import WikiToPageHeadsService, HeadsCountService, TopHeadsService
from nlp_services.discourse.entities import WikiEntitiesService, WpWikiEntitiesService, CombinedWikiEntitiesService, TopEntitiesService, WpTopEntitiesService, CombinedTopEntitiesService, WikiPageEntitiesService, WpWikiPageEntitiesService, CombinedWikiPageEntitiesService, EntityDocumentCountsService, WpEntityDocumentCountsService, CombinedDocumentEntityCountsService, WikiPageToEntitiesService, WpPageToEntitiesService, CombinedPageToEntitiesService
from nlp_services.discourse import AllEntitiesSentimentAndCountsService
from nlp_services.discourse.sentiment import WikiEntitySentimentService, WpWikiEntitySentimentService
from title_confirmation.wikia import AllTitlesService, RedirectsService

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

#todo argparse
service_file = 'services-config.json'
SERVICES = json.loads(open(service_file).read())['wiki-services']

caching_dict = dict([(service+'.get', {'write_only': True}) for service in SERVICES])
use_caching(per_service_cache=caching_dict)

wid = sys.argv[1]
try:
    for service in SERVICES:
        try:
            print service
            getattr(sys.modules[__name__], service)().get(wid)
            caching_dict[service+'.get'] = {'dont_compute': True}  # DRY fool!
            use_caching(per_service_caching=caching_dict)
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            print 'Could not call %s on %s!' % (service, wid)
            print traceback.format_exc()
except:
    print "Problem with", wid
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

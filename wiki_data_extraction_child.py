import sys
import traceback
import os
import json
from boto import connect_s3
from nlp_client.services import *
from nlp_client.caching import useCaching

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

#todo argparse
service_file = 'services-config.json'
SERVICES = json.loads(open(service_file).read())['wiki-services']

caching_dict = dict([(service+'.get', {'write_only': True}) for service in SERVICES])
useCaching(perServiceCaching=caching_dict)

if len(sys.argv) > 2:
    use_multiprocessing(num_cores=int(sys.argv[2]))

wid = sys.argv[1]
try:
    for service in SERVICES:
        try:
            print service
            getattr(sys.modules[__name__], service)().get(wid)
            caching_dict[service+'.get'] = {'dont_compute': True}  # DRY fool!
            useCaching(perServiceCaching=caching_dict)
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            print 'Could not call %s on %s!' % (service, wid)
            print traceback.format_exc()
except:
    print "Problem with", wid
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

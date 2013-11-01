import sys
import traceback
import os
import json
from boto import connect_s3
from nlp_client.services import *
from nlp_client.caching import useCaching

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

service_file = sys.argv[2] if len(sys.argv) > 2 else 'services-config.json'
SERVICES = json.loads(open(service_file).read())['wiki-services']

caching_dict = dict([(service+'.get', {'write_only': True}) for service in SERVICES])
caching_dict = {}
useCaching(perServiceCaching=caching_dict)

wid = sys.argv[1]
try:
    for service in SERVICES:
        try:
            print service
            getattr(sys.modules[__name__], service)().get(wid)
            caching_dict[service+'.get'] = {'dont_compute': True}  # DRY fool!
            useCaching(caching_dict)
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            print 'Could not call %s on %s!' % (service, wid)
            print traceback.format_exc()
except:
    print "Problem with", wid
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

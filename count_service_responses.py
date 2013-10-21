from boto import connect_s3
from boto.s3.prefix import Prefix
from multiprocessing import Pool

bucket = connect_s3().get_bucket('nlp-data')

wids = [prefix.name.split('/')[-2] for prefix in connect_s3().get_bucket('nlp-data').list(prefix='xml/', delimiter='/') if isinstance(prefix, Prefix)]

def f(x):
    return 1 if bucket.get_key('service_responses/%s/TopEntitiesService.get' % x) is not None else 0

print len(wids), '/', sum(Pool(processes=4).map(f, wids))

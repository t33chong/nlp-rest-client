# Attempt to make sense of topic features by identifying the top entities
# common to them across different wikis

import logging
import requests
import sys
import traceback
from collections import defaultdict
from multiprocessing import Pool
from nlp_client.caching import useCaching
from nlp_client.services import TopEntitiesService
from wiki_recommender import as_euclidean, get_topics_sorted_keys

SOLR_URL = 'http://dev-search.prod.wikia.net:8983/solr/xwiki/select'

useCaching(dontCompute=True)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
fh = logging.FileHandler('lda_clustering.log')
fh.setLevel(logging.ERROR)
log.addHandler(fh)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
log.addHandler(sh)

def get_keys_and_entities(wid):
    """Given a wiki id, return a tuple containing:
    0. A list of keys (topics) for which term frequency is non-zero
    1. A cumulative list of top entities from all related wikis"""
    entities = requests.get(SOLR_URL, params={'q': 'id:%s' % wid, 'fl': 'entities_txt', 'wt': 'json'}).json()['response']['docs'][0].get('entities_txt', [])
    doc, docs = as_euclidean(wid)
    related_wids = [wiki['id'] for wiki in docs]
    for related_wid in related_wids:
        entities.extend(requests.get(SOLR_URL, params={'q': 'id:%s' % related_wid, 'fl': 'entities_txt', 'wt': 'json'}).json()['response']['docs'][0].get('entities_txt', []))
    keys = get_topics_sorted_keys(doc)
    return (keys, entities)

def cluster(wid):
    log.info('Clustering wid ' + wid)
    try:
        keys, entities = get_keys_and_entities(wid)
    except KeyboardInterrupt:
        sys.exit(0)
    except:
        log.error(wid + ':\n' + traceback.format_exc())
        return wid + 'failed'
    # Create a dict with entity strings as keys, and tallies as values
    tally = dict(zip(entities, map(entities.count, entities)))
    return (keys, tally)

# Instantiate nested defaultdicts, with values of the inner defaulting to 0
topics = defaultdict(lambda: defaultdict(int))

count = 0
# Iterate over top 5k wikis
wids = [line.strip() for line in open('topwams.txt').readlines()[:5000]]
for (keys, tally) in Pool(processes=8).map(cluster, wids):
    print 'Count:', count
    # Increment topics dict w/ totals; keep running tally of entities per key
    for key in keys:
        for entity in tally:
            topics[key][entity] += tally[entity]

# Write most frequent entities per topic feature to CSV
with open('clustered.csv', 'w') as f:
    for topic in topics:
        s = sorted(topics[topic].items(), key=lambda x: x[1], reverse=True)
        f.write(','.join([topic] + [k for (k, v) in s[:25]]).encode('utf-8') + '\n')

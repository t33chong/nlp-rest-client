import requests,sys
from time import sleep
from subprocess import Popen
from random import shuffle

r = requests.get('http://search-s10:8983/solr/xwiki/select', params={'wt':'json', 'q':'lang_s:en', 'sort': 'wam_i desc', 'rows':'10000', 'fl':'id'})
ids = [doc['id'] for doc in r.json()['response']['docs']]

shuffle(ids)
processes = []
while len(ids) > 1:
    while len(processes) < 8:
        id = ids.pop()
        #print id
        processes += [Popen('python import_entities.py %s %s' % (id, 'topentities.sql'), shell=True)]
    processes = [x for x in processes if x.poll() is None]
    sleep(0.5)
    

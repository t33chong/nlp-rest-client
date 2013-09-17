import requests
from subprocess import Popen

url = 'http://search-s10:8983/solr/xwiki/select/'
params = {'fl':'id', 'q':'lang_s:en', 'sort':'wam_i desc', 'rows':0, 'start':0, 'wt':'json'}
numFound = requests.get(url, params=params).json().get('response', {}).get('numFound')

params['rows'] = 10
processes = []
for i in range(0, numFound, 10):
    params['start'] = i
    docs = requests.get(url, params=params).json().get('response', {}).get('docs', [])
    processes = map(lambda x:Popen(['python', 'title_data_harvester.py', x['id']]), docs)
    while len(processes) > 0:
        processes = filter(lambda x:x.poll() is None, processes)

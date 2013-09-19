"""
Responsible for writing text files, either for single documents, or for all content
articles in a given wiki.
Can write to a local machine or to an Amazon AWS S3 bucket.

Query queue filepath is provided as sys.argv[1]
Variable indicating local or S3 writing is provided as sys.argv[2]
"""

import os
import sys
from utils import clean_list, ensure_dir_exists
from WikiaSolr.queryiterator import QueryIterator

TEXT_DIR = '/data/text/'

qqfile = sys.argv[1]
local = bool(int(sys.argv[2]))

#print qqfile, local

batch_count = 0
doc_count = 0

#TODO: add last indexed option
for line in open(qqfile):
    query = line.strip()
    qi = QueryIterator('http://search-s11.prod.wikia.net:8983/solr/main/select', {'query': query, 'fields': 'id,html_en,indexed', 'sort': 'id asc'})
    for doc in qi:
        if batch_count % 250 == 0:
            batch_count += 1
            dest_dir = TEXT_DIR + '%s_%i' % (os.path.basename(qqfile), batch_count)
            #TODO: continue here, mirror write_text() from nlp-harvester


#HOST = 'http://search-s11.prod.wikia.net:8983/solr/main/select'
#
#class TextWriter(object):
#    def __init__(self, lang='en', local=False, destination='/data'):
#        self.language = lang
#        self.destination = ensure_dir_exists(destination)
#        if not local:
#            aws = json.loads(open('aws.json').read())
#            self.aws_key = aws['key']
#            self.aws_secret = aws['secret']
#            self.destination = """Implement writing to S3"""
#
#    def get_single_doc(self, wid_or_id):
#        if '_' in str(wid_or_id):
#            return json.loads(requests.get(HOST, params={'q': 'id:%s' % wid_or_id, 'fl': 'html_en', 'wt': 'json'}).content)['response']['docs'][0].get('html_%s' % self.language, '')
#
#    def write_to_aws(self, text):
#        pass
#
#    def write_to_local(self, text):
#        pass
#
#

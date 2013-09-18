"""
Responsible for writing text files, either for single documents, or for all content
articles in a given wiki.
Can write to a local machine or to an Amazon AWS S3 bucket.

REMOVE THE FOLLOWING:
Wiki ID is provided as sys.argv[1]
Language is optionally provided as sys.argv[2]
Last indexed condition is optionally provided as sys.argv[3]
"""
import os
import sys
import requests
from WikiaSolr.queryiterator import QueryIterator
from utils import clean_list, ensure_dir_exists

HOST = 'http://search-s11.prod.wikia.net:8983/solr/main/select'

class TextWriter(object):
    def __init__(self, lang='en', local=False, destination='/data'):
        self.language = lang
        self.destination = ensure_dir_exists(destination)
        if not local:
            aws = json.loads(open('aws.json').read())
            self.aws_key = aws['key']
            self.aws_secret = aws['secret']
            self.destination = """Implement writing to S3"""

    def get_single_doc(self, wid_or_id):
        if '_' in str(wid_or_id):
            return json.loads(requests.get(HOST, params={'q': 'id:%s' % wid_or_id, 'fl': 'html_en', 'wt': 'json'}).content)['response']['docs'][0].get('html_%s' % self.language, '')

    def write_to_aws(self, text):
        pass

    def write_to_local(self, text):
        pass


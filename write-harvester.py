"""
Responsible for writing text files, either for single documents, or for all content
articles in a given wiki.
Can write to a local machine or to an Amazon AWS S3 bucket.

Query queue filepath is provided as sys.argv[1]
Variable indicating local or S3 writing is provided as sys.argv[2]
"""

import os
import sys
import shutil
import tarfile
from utils import clean_list, ensure_dir_exists
from WikiaSolr.queryiterator import QueryIterator

TEXT_DIR = '/data/text/'

qqfile = sys.argv[1]
local = bool(int(sys.argv[2]))

#print qqfile, local

batch_count = 0
doc_count = 0
tar = None

#TODO: add last indexed option
for line in open(qqfile):
    query = line.strip()
    print query
    qi = QueryIterator('http://search-s11.prod.wikia.net:8983/solr/main/', {'query': query, 'fields': 'id,html_en,indexed', 'sort': 'id asc'})
    for doc in qi:
        print doc.get('id')
        if doc_count % 250 == 0:
            if tar:
                tar.close()
                shutil.rmtree(dest_dir)
            batch_count += 1
            dest_dir = ensure_dir_exists(TEXT_DIR + '%s_%i' % (os.path.basename(qqfile), batch_count))
            tar = tarfile.open(dest_dir + '.tgz', 'w:gz')
        text = '\n'.join(clean_list(doc.get('html_en', '')))
        localpath = os.path.join(dest_dir, doc['id'])
        with open(localpath, 'w') as f:
            f.write(text)
        tar.add(localpath, doc['id'])
        doc_count += 1
if tar:
    tar.close()
    shutil.rmtree(dest_dir)

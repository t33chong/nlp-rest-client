"""
Responsible for writing text files, either for single documents, or for all content
articles in a given wiki.
Can write to a local machine or to an Amazon AWS S3 bucket.

Query queue filepath is provided as sys.argv[1]
Integer indicating local or S3 writing is provided as sys.argv[2]
"""

import os
import sys
import json
import shutil
import tarfile
import requests
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from utils import clean_list, ensure_dir_exists
from WikiaSolr.queryiterator import QueryIterator

TEXT_DIR = '/data/text/'

qqfile = sys.argv[1]
aws = bool(int(sys.argv[2]))

if aws:
    bucket = S3Connection().get_bucket('nlp-data')
    k = Key(bucket)

batch_count = 0
doc_count = 0
tar = None
wids = []

#TODO: add last indexed option
for line in open(qqfile):
    query = line.strip()
    print query
    qi = QueryIterator('http://search-s11.prod.wikia.net:8983/solr/main/', {'query': query, 'fields': 'id,wid,html_en,indexed', 'sort': 'id asc'})
    for doc in qi:
        # write files across all queries in qqfile to batches of 250
        if doc_count % 250 == 0:
            # skip until the first batch has completed
            if tar:
                tar.close()
                # remove text files after tarring
                shutil.rmtree(dest_dir)
                # send to aws and remove tarball
                if aws:
                    k.key = 'text_events/%s' % os.path.basename(tar_file)
                    k.set_contents_from_filename(tar_file)
                    os.remove(tar_file)
                    # send post requests for each wid covered in this batch
                    for wid in wids:
                        requests.post('http://nlp-s1:5000/wiki/%i' % wid)
                wids = []
            batch_count += 1
            dest_dir = ensure_dir_exists(TEXT_DIR + '%s_%i' % (os.path.basename(qqfile), batch_count))
            # open tarball for writing
            tar_file = dest_dir + '.tgz'
            tar = tarfile.open(tar_file, 'w:gz')
        wid = int(doc['wid'])
        if wid not in wids:
            wids.append(wid)
        # sanitize and write text
        text = '\n'.join(clean_list(doc.get('html_en', '')))
        localpath = os.path.join(dest_dir, doc['id'])
        with open(localpath, 'w') as f:
            f.write(text)
        # add text file to tarball
        tar.add(localpath, doc['id'])
        doc_count += 1
# tar the final batch and send to aws
if tar:
    tar.close()
    shutil.rmtree(dest_dir)
    if aws:
        k.key = 'text_events/%s' % os.path.basename(tar_file)
        k.set_contents_from_filename(tar_file)
        os.remove(tar_file)
        for wid in wids:
            requests.post('http://nlp-s1:5000/wiki/%i' % wid)

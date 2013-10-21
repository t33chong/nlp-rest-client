from boto import connect_s3
from boto.s3.key import Key
from nlp_client.title_confirmation import preprocess
from urllib import quote_plus
import gzip
import sys
import os


bucket = connect_s3().get_bucket('nlp-data')

trie = {}

key = None
file_for_key = None
print "Extracting titles..."
for title in sorted([(quote_plus(preprocess(line)[:4].replace("\n", "")), preprocess(line[:-1])) for line in gzip.open(sys.argv[1]).readlines() if len(line.replace("\n", "")) >= 2], key=lambda x: x[0]):
    print title
    expected_key = 'wikipedia_titles/'+title[0]+'.gz'
    if key is None or key.key != expected_key:
        if key is not None and file_for_key is not None:
            file_for_key.close()
            key.set_contents_from_filename('/tmp/'+key.key)
            os.remove('/tmp/'+key.key)
            del key
            del file_for_key
            print "done"
        print title[0], "..."
        key = Key(bucket)
        key.key = expected_key
        file_for_key = gzip.open('/tmp/'+expected_key, 'w')
    file_for_key.write(title[1]+"\n")
    
file_for_key.close()
key.set_contents_from_filename('/tmp/'+key.key)
os.remove('/tmp/'+key.key)
del key
del file_for_key
print "done"

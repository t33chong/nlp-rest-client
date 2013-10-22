from nlp_client import preprocess, make_trie
from boto import connect_s3
from boto.s3.key import Key
import sys
import gzip
import json

k = Key(connect_s3().get_bucket('nlp-client'))
f = gzip.open('/tmp/trie.gz')
f.write(json.dumps(make_trie[preprocess(i.strip()) for i in gzip.open(sys.argv[1]).readlines()], ensure_ascii=False))
k.set_contents_from_file(f)

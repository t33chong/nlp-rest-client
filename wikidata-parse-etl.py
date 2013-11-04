import requests
import sys
import traceback
import json
from multiprocessing import Pool


def etl_for_wiki(wid):
    wid = wid.strip()
    r = requests.get(u'http://search-s10:8983/solr/xwiki/select/', 
                           params=dict(wt=u'json', q=u'id:%s' % wid, fl=u'id,description_txt,sitename_txt,headline_txt', rows=1))

    response = r.json()

    if int(response[u'responseHeader'][u'status']) != 0:
        return

    docs = response[u'response'][u'docs']

    if len(docs) == 0:
        return

    doc = docs[0]
    
    def write_file(doc, field):
        def as_string(field_value):
            return "\n".join(field_value) if not isinstance(field_value, basestring) else field_value

        st = as_string(doc[field])
        if len(st) > 0:
            with open(u'/data/wiki_text/%s_%s' % (doc[u'id'], field), u'w') as fl:
                fl.write(st.encode('UTF-8'))

    [write_file(doc, field) for field in doc if field != u'id']

def wrapped_etl_for_wiki(wid):
    try:
        etl_for_wiki(wid)
    except UnicodeDecodeError as e:
        print wid, "had unicode issues:", e
        traceback.print_stack()
    except UnicodeEncodeError as e:
        print wid, "had unicode issues:", e
        traceback.print_stack()

Pool(processes=int(sys.argv[1])).map(etl_for_wiki, open('topwams.txt'))

"""
Responsible for retrieving a list of top entities, given a wiki ID, and returning it to entity-harvester.

Wiki ID is provided as sys.argv[1]
"""

import os
import sys
from subprocess import Popen
from nlp_client.services import SolrWikiService, TopEntitiesService
from _mysql_exceptions import OperationalError

def main(wid):
    try:
        response = TopEntitiesService().get(wid)[wid]
        entities = [entity[0].decode('utf-8') for entity in response]
        url = SolrWikiService().get(int(wid))[int(wid)]['url'].decode('utf-8')
        print url.encode('utf-8') + ',' + ','.join(entities).encode('utf-8')
    except:
        raise
    finally:
        Popen('python %s -w %s' % (os.path.join(os.getcwd(), 'purge-cassandra.py'), wid), shell=True)

if __name__ == '__main__':
    wid = sys.argv[1]
    main(wid)

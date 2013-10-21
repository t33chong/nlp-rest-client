import sys
import phpserialize
from wikicities import LoadBalancer
from nlp_client.services import TopEntitiesService, ListDocIdsService, EntityCountsService
from nlp_client.title_confirmation import get_global_db, get_local_db_from_wiki_id
from nlp_client.caching import useCaching


WIKI_ENTITIES_ID = 1339
PAGE_PROP_ENTITIES = 22

wid = sys.argv[1]
fname = sys.argv[2]

useCaching()

sqlfile = open(fname, 'a')

"""
counts = EntityCountsService()
localdb = get_local_db_from_wiki_id(get_global_db(), wid, master=True)

values = []
for docId in ListDocIdsService().nestedGet(wid):
    try:
        response = counts.get(docId)
        if response.get('status') == 200:
            top_entities = sorted(response[docId].items(), key=lambda x: x[1], reverse=True)[:5]
            values += (docId, top_entities)
    except:
        pass
    
for i in range(0, len(values), 10):
    localdb.cursor().execute("INSERT INTO `page_wikia_props` (`page_id`, `propname`, `props`) VALUES " % ", ".join(map(lambda x: (x[0], PAGE_PROP_ENTITIES, x[1]), values[i:i+10])))
"""

response = TopEntitiesService().get(wid)
if response.get('status') == 200:
    top_entities = [x[0] for x in response[wid][:5]]
    if len(top_entities) > 0:
        dumped = phpserialize.dumps(top_entities).replace('"', '\\"')
        sql = "INSERT INTO `city_variables` (`cv_city_id`, `cv_variable_id`, `cv_value`) VALUES (%s, %d, \"%s\");" % (wid, WIKI_ENTITIES_ID, dumped)
        print sql
        #get_global_db(master=True).cursor().execute(sql)
    else:
        pass#wid, "no entities"

sys.exit()

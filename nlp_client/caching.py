from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
import json
import time

'''
Caching library -- basically memoizes stuff for now
'''

'''
If this value gets initialized then we will start memoizing things witih DynamoDB
'''
CACHE_TABLE = None

''' Needed to enumerate, unfortunately '''
CACHED_SERVICES = ['CoreferenceCountsService.get', 'AllNounPhrasesService.get', 'AllVerbPhrasesService.get', 
                   'HeadsService.get', 'HeadsCountService.get', 'TopHeadsService.get', 'SentimentService.get',
                   'AllTitlesService.get', 'RedirectsServices.get', 'EntitiesService.get', 'EntityCountsService.get',
                   'TopEntitiesService.get', 'WikiEntitiesService.get']

def table(table=None):
    ''' Access & mutate so we don't have globals in every function
    :param table: An instance of boto.dyanmodb2.table.Table
    '''
    global CACHE_TABLE
    if table:
        CACHE_TABLE = table
    return CACHE_TABLE


def useCaching():
    ''' Invoke this to set CACHE_TABLE and enable caching on these services '''
    table(Table('service_data'))
    

def purgeCacheForDoc(doc_id):
    ''' Remove all service responses for a given doc id
    :param doc_id: the document id. if it's a wiki id, you're basically removing all wiki-scoped caching
    '''
    with table().batch_write() as batch:
        map(lambda x: batch.delete_item(doc_id=doc_id, service=x), CACHED_SERVICES)

    return True

def purgeCacheForService(service_and_method):
    ''' Remove cached service responses for a given service
    :param service_and_method: the ServiceName.method
    '''
    with table().batch_write() as batch:
        map(lambda x: x.delete(),
            table().query(service__eq=service_and_method))

    return True


def purgeCacheForWiki(wiki_id):
    ''' Remove cached service responses for a given wiki id
    :param wiki_id: the id of the wiki
    '''
    global CACHED_SERVICES

    map(lambda y: map(lambda x: x.delete(), y), 
        map(lambda z: table().query(service__eq=z, wiki_id__eq=int(wiki_id), index='wiki_id-index'), 
            CACHED_SERVICES))

    return True


def cachedServiceRequest(getMethod):
    ''' This is a decorator responsible for optionally memoizing a service response into the cache
    :param getMethod: the function we're wrapping -- should be a GET endpoint
    '''
    def invoke(self, *args, **kw):

        doc_id = kw.get('doc_id', kw.get('wiki_id', None))
        if not doc_id:
            doc_id = args[0]
        wiki_id = int(doc_id.split('_')[0])
        service = str(self.__class__.__name__)+'.'+getMethod.func_name
        if not table():
            response = getMethod(self, *args, **kw)
        else:
            
            data = {'doc_id':doc_id, 'service':service, 'wiki_id':wiki_id}

            results = [result for result in table().query(service__eq=service, doc_id__eq=doc_id)]

            if len(results) == 0:
                response = getMethod(self, *args, **kw)
                if response['status'] == 200:
                    data['response'] = json.dumps(response[doc_id])
                    data['updated'] = int(time.time())
                    item = Item(table(), data)
                    item.save(overwrite=True)
            else:
                response = dict(results[0])
                response[doc_id] = json.loads(response['response'])
                del response['response']

        return response
    return invoke

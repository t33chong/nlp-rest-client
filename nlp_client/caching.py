import cql
import json
import time

'''
Caching library -- basically memoizes stuff for now
'''

'''
If this value gets initialized then we will start memoizing things with Cassandra
'''
CACHE_DB = None

WRITE_ONLY = False
READ_ONLY = False

def db(new_db = None):
    ''' Access & mutate so we don't have globals in every function
    :param new_db:cql connection
    '''
    global CACHE_DB
    if new_db:
        CACHE_DB = new_db
    return CACHE_DB


def read_only(mutate = None):
    ''' Access & mutate so we don't need globals in every function
    :param mutate: a boolean value
    '''
    global READ_ONLY
    if mutate is not None:
        READ_ONLY = mutate
    return READ_ONLY


def write_only(mutate = None):
    ''' Access & mutate so we don't need globals in every function
    :paramm mutate: a boolean value
    '''
    global WRITE_ONLY
    if mutate is not None:
        WRITE_ONLY = mutate
    return WRITE_ONLY


def useCaching(host='dev-indexer-s1', port=9160, keyspace='nlp', writeOnly = False, readOnly = False):
    ''' Invoke this to set CACHE_DB and enable caching on these services 
    :param write_only: whether we should avoid reading from the cache
    :param read_only: whether we should avoid writing to the cache
    '''
    db(new_db=cql.connection.connect(host, port, keyspace))
    read_only(readOnly)
    write_only(writeOnly)


def purgeCacheForDoc(doc_id):
    ''' Remove all service responses for a given doc id
    :param doc_id: the document id. if it's a wiki id, you're basically removing all wiki-scoped caching
    '''
    cursor = db().cursor()
    result = cursor.execute("SELECT signature FROM service_responses WHERE doc_id = :doc_id", params={'doc_id':doc_id})

    return purgeCacheForSignatures([result[0] for result in cursor])


def purgeCacheForService(service_and_method):
    ''' Remove cached service responses for a given service
    :param service_and_method: the ServiceName.method
    '''
    cursor = db().cursor()
    cursor.execute("SELECT signature FROM service_responses WHERE service = :service", params={'service':service_and_method})
    return purgeCacheForSignatures([result[0] for result in cursor])


def purgeCacheForWiki(wiki_id):
    ''' Remove cached service responses for a given wiki id
    :param wiki_id: the id of the wiki
    '''
    cursor = db().cursor()
    cursor.execute("SELECT signature FROM service_responses WHERE wiki_id = :wiki_id", params={'wiki_id':wiki_id})
        
    return purgeCacheForSignatures([result[0] for result in cursor])


def purgeCacheForSignatures(signatures):
    ''' Bulk delete for multiple signatures
    :param signatures: list of signatures
    '''
    prepared = []
    counter = 1
    params = {}

    for signature in signatures:
        currkey = "signature"+str(counter)
        prepared += [':'+currkey]
        params[currkey] = signature
        counter += 1
        if counter % 20 == 0:
            cursor.execute("DELETE FROM service_responses WHERE signature IN (%s)" % (", ".join(prepared)), params=params)
            prepared, params = [], {}

    return True


def cachedServiceRequest(getMethod):
    ''' This is a decorator responsible for optionally memoizing a service response into the cache
    :param getMethod: the function we're wrapping -- should be a GET endpoint
    '''
    def invoke(self, *args, **kw):

        connection = db()
        if not connection:
            response = getMethod(self, *args, **kw)

        else:
            doc_id = kw.get('doc_id', kw.get('wiki_id', None))
            if not doc_id:
                doc_id = args[0]
            wiki_id = int(doc_id.split('_')[0])
            service = str(self.__class__.__name__)+'.'+getMethod.func_name
            
            data = {'doc_id':doc_id, 'service':service, 'wiki_id':wiki_id}
            signature = json.dumps(data)

            cursor = connection.cursor()

            result = None
            if not write_only():
                query = """
                      SELECT response
                      FROM service_responses
                      WHERE signature = :signature
                """
                if cursor.execute(query, params={'signature':signature}):
                    result = cursor.fetchone()

            if len(result) < 1 or result[0] is None:
                response = getMethod(self, *args, **kw)
                if response['status'] == 200 and not read_only():
                    insert = """
                           INSERT INTO service_responses (signature, doc_id, service, wiki_id, response, last_updated)
                           VALUES (:signature, :doc_id, :service, :wiki_id, :response, :last_updated)
                    """
                    data['signature'] = signature
                    data['response'] = json.dumps(response)
                    data['last_updated'] = int(time.time())
                    cursor.execute(insert, params=data)

            else:
                response = json.loads(result[0])

        return response
    return invoke

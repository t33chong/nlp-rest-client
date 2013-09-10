import cql
import json

'''
Caching library -- basically memoizes stuff for now
'''

'''
If this value gets initialized then we will start memoizing things witih Cassandra
'''
CASSANDRA_CLIENT = None


def useCaching(host='dev-indexer-s1', port=9160, keyspace='nlp'):
    ''' Invoke this to set REDIS_CLIENT and enable caching on these services
    :param host: redis server hostname
    :param port: redis server port
    :param db: redis db name
    '''
    global CASSANDRA_CLIENT
    CASSANDRA_CLIENT = cql.connection.connect(host, port, keyspace)

def purgeCacheForDoc(doc_id):
    ''' Remove all service responses for a given doc id
    :param doc_id: the document id. if it's a wiki id, you're basically removing all wiki-scoped caching
    '''
    global CASSANDRA_CLIENT
    cursor = CASSANDRA_CLIENT.cursor()
    result = cursor.execute("SELECT signature FROM service_responses WHERE doc_id = :doc_id", params={'doc_id':doc_id})
    deletes = [row[0] for row in cursor]
    map(lambda x: cursor.execute("DELETE FROM service_responses WHERE signature = :signature", params={'signature':x[0]}), deletes)
    return True

def purgeCacheForService(service_and_method):
    ''' Remove cached service responses for a given service
    :param service_and_method: the ServiceName.method
    '''
    global CASSANDRA_CLIENT
    cursor = CASSANDRA_CLIENT.cursor()
    result = cursor.execute("SELECT signature FROM service_responses WHERE service = :service", params={'service':service_and_method})
    deletes = [row[0] for row in cursor]
    map(lambda x: cursor.execute("DELETE FROM service_responses WHERE signature = :signature", params={'signature':x[0]}), deletes)
    return True

def purgeCacheForWiki(wiki_id):
    ''' Remove cached service responses for a given wiki id
    :param wiki_id: the id of the wiki
    '''
    global CASSANDRA_CLIENT
    cursor = CASSANDRA_CLIENT.cursor()
    result = cursor.execute("SELECT signature FROM service_responses WHERE wiki_id = :wiki_id", params={'wiki_id':wiki_id})
    deletes = [row[0] for row in cursor]
    map(lambda x: cursor.execute("DELETE FROM service_responses WHERE signature = :signature", params={'signature':x[0]}), deletes)
    return True

def cachedServiceRequest(getMethod):
    ''' This is a decorator responsible for optionally memoizing a service response into the cache
    :param getMethod: the function we're wrapping -- should be a GET endpoint
    '''
    def invoke(self, *args, **kw):
        global CASSANDRA_CLIENT
        doc_id = kw.get('doc_id', kw.get('wiki_id', None))
        if not doc_id:
            doc_id = args[0]
        wiki_id = doc_id.split('_')[0]
        service = str(self.__class__.__name__)+'.'+getMethod.func_name
        if not CASSANDRA_CLIENT:
            response = getMethod(self, *args, **kw)
        else:
            cursor = CASSANDRA_CLIENT.cursor()
            query = """
                  SELECT response
                  FROM service_responses
                  WHERE signature = :signature
            """
            data = {'doc_id':doc_id, 'service':service, 'wiki_id':wiki_id}
            signature = json.dumps({'service':service, 'doc_id':doc_id, 'wiki_id': wiki_id}) # could hash this
            cursor.execute(query, params={'signature':signature})
            result = cursor.fetchone()
            if len(result) < 1 or result[0] is None:
                response = getMethod(self, *args, **kw)
                if response['status'] == 200:
                    insert = """
                           INSERT INTO service_responses (signature, doc_id, service, wiki_id, response)
                           VALUES (:signature, :doc_id, :service, :wiki_id, :response)
                    """
                    data['signature'] = signature
                    data['response'] = json.dumps(response)
                    cursor.execute(insert, params=data)
            else:
                response = json.loads(result[0])
        return response
    return invoke

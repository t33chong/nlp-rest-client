from flask.ext import restful
from text.blob import TextBlob
from nlp_client.services import *
from os import path, listdir
from gzip import open as gzopen
import cql
import re
import nltk
import xmltodict
import requests
import redis
import types
import json
import sys


'''
This module contains all services used in our RESTful client.
At this point, they are all read-only, and only respond to GET.
'''

'''
If this value gets initialized then we will start memoizing things witih Cassandra
'''
CASSANDRA_CLIENT = None

XML_PATH = '/data/xml/'

# TODO: use load balancer, not a partiucular query slave
SOLR_URL = 'http://search-s10:8983'

MEMOIZED_WIKIS = {}
MEMOIZED_ENTITIES = {}

def useCaching(host='dev-indexer-s1', port=9160, keyspace='nlp'):
    ''' Invoke this to set REDIS_CLIENT and enable caching on these services
    :param host: redis server hostname
    :param port: redis server port
    :param db: redis db name
    '''
    global CASSANDRA_CLIENT
    CASSANDRA_CLIENT = cql.connection.connect(host, port, keyspace)

def cachedServiceRequest(getMethod):
    ''' This is a decorator responsible for optionally memoizing a service response into the cache
    :param getMethod: the function we're wrapping -- should be a GET endpoint
    '''
    def invoke(self, *args, **kw):
        global CASSANDRA_CLIENT
        doc_id = args[0]
        wiki_id = args[0].split('_')[0] if args[0].isdigit() else 0
        service = str(self.__class__)+'.'+getMethod.func_name
        if not CASSANDRA_CLIENT:
            response = getMethod(self, *args)
        else:
            cursor = CASSANDRA_CLIENT.cursor()
            query = """
                  SELECT response
                  FROM service_responses
                  WHERE doc_id_and_service = :doc_id_and_service
            """
            doc_id_and_service = doc_id+'_'+service
            params = params={'doc_id_and_service': doc_id_and_service}
            cursor.execute(query, params=params)
            result = cursor.fetchone()
            if len(result) < 1:
                response = getMethod(self, *args)
                if response['status'] == 200:
                    insert = """
                           INSERT INTO service_responses (doc_id_and_service, doc_id, service, wiki_id, response)
                           VALUES (:doc_id_and_service, :doc_id, :service, :wiki_id, :response)
                    """
                    params['doc_id'] = doc_id
                    params['service'] = service
                    params['wiki_id'] = wiki_id
                    params['response'] = json.dumps(response)
                    cursor.execute(insert, params=params)
            else:
                response = json.loads(result[0])
        return response
    return invoke


class ParsedXmlService(restful.Resource):

    ''' Read-only service responsible for accessing XML from FS '''
    
    def get(self, doc_id):
        ''' Return a response with the XML of the parsed text 
        :param doc_id: the id of the document in Solr
        '''

        response = {}
        (wid, id) = doc_id.split('_')
        xmlPath = '%s/%s/%s/%s.xml' % (XML_PATH, wid, id[0], id)
        gzXmlPath = xmlPath + '.gz'
        if path.exists(gzXmlPath):
            response['status'] = 200
            response[doc_id] = ''.join(gzopen(gzXmlPath).readlines())
        elif path.exists(xmlPath):
            response['status'] = 200
            response[doc_id] = ''.join(open(xmlPath).readlines())
        else:
            response['status'] = 500
            response['message'] = 'File not found for document %s' % doc_id
        return response


class ParsedJsonService(restful.Resource):

    ''' Read-only service responsible for accessing XML and transforming it to JSON
    Uses the ParsedXmlService
    '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Returns document parse as JSON 
        :param doc_id: the id of the document in Solr
        '''

        response = {}
        xmlResponse = ParsedXmlService().get(doc_id)
        if xmlResponse['status'] != 200:
            return xmlResponse
        return {'status':200, doc_id: xmltodict.parse(xmlResponse[doc_id])}


class CoreferenceCountsService(restful.Resource):

    ''' Read-only service responsible for providing data on mention coreference
    Uses the ParsedJsonService
    '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Returns coreference and mentions for a document 
        :param doc_id: the id of the document in Solr
        '''
        
        response = {}
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse

        doc = jsonResponse[doc_id]
        if isEmptyDoc(doc):
            return {'status':200, doc_id:{}, 'message':'Document was empty'}
            
        coreferences = asList(doc['root']['document'].get('coreference', {}).get('coreference', []))
        sentences = asList(doc['root']['document'].get('sentences', {}).get('sentence', []))

        mentionCounts = {}
        representativeToMentions = {}
        for coref in coreferences:
            mentionString = ''
            mentionCount = 0
            mentions = []
            for mention in coref.get('mention', []): #
                mentionCount += 1
                try:
                    currentMentionString = " ".join([token['word'] for token in sentences[int(mention['sentence'])-1]['tokens']['token'][int(mention['start'])-1:int(mention['end'])-1]]) 
                    if mention.get('@representative', 'false') == 'true':
                        mentionString = currentMentionString
                    mentions += [currentMentionString]
                except TypeError: #unhashable type -- but why?
                    continue
                except KeyboardInterupt:
                    sys.exit()
            mentionCounts[mentionString] = mentionCount
            representativeToMentions[mentionString] = mentions
        return {doc_id: {'mentionCounts':mentionCounts, 'paraphrases': representativeToMentions}, 'status':200}



class AllNounPhrasesService(restful.Resource):

    ''' Read-only service that gives all noun phrases for a document
    TextBlob could do this too
    Uses ParsedJsonService
    '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Get noun phrases for a document 
        :param doc_id: the id of the document in Solr
        '''
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse
        dict = jsonResponse[doc_id]
        nps = []
        if not isEmptyDoc(dict):
            sentences = asList(dict.get('root', {}).get('document', {}).get('sentences', {}).get('sentence', []))
            for sentence in sentences:
                nps += [' '.join(f.leaves()) for f in nltk.Tree.parse(sentence.get('parse', '')).subtrees() if f.node == u'NP']
        return {doc_id:nps, 'status':200}


class SolrPageService(restful.Resource):

    ''' Read-only service that accesses a single page-level document from Solr '''
    
    def get(self, doc_id):
        ''' Get page from solr for a document id 
        :param doc_id: the id of the document in Solr
        '''
        return {doc_id: requests.get(SOLR_URL+'/solr/main/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0], 'status':200}


class SolrWikiService(restful.Resource):

    ''' Read-only service that accesses a single wiki-level document from Solr '''
    
    def get(self, wiki_id):
        ''' Get wiki from solr for a document id
        :param doc_id: the id of the document in Solr
        '''
        global MEMOIZED_WIKIS
        if MEMOIZED_WIKIS.get(wiki_id, None):
            return {wiki_id: MEMOIZED_WIKIS[wiki_id]}

        serviceResponse = {wiki_id: requests.get(SOLR_URL+'/solr/xwiki/select/', params={'q':'id:%s' % wiki_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0], 'status':200}

        MEMOIZED_WIKIS = dict(MEMOIZED_WIKIS.items() + serviceResponse.items())

        return serviceResponse


class SentimentService(restful.Resource):

    ''' Read-only service that calculates the sentiment for a given piece of text
    Relies on SolrPageService
    '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' For a document id, get data on the text's polarity and subjectivity 
        :param doc_id: the id of the document in Solr
        '''
        blob = TextBlob(SolrPageService().get(doc_id).get(doc_id, {}).get('html_en', ''))
        sentiments = [s.sentiment for s in blob.sentences]
        polarities = [s[0] for s in sentiments]
        subjectivities = [s[1] for s in sentiments]
        sentimentData = {}
        sentimentData['polarity_avg'] = sum(polarities)/float(len(sentiments))
        sentimentData['polarity_max'] = max(polarities)
        sentimentData['polarity_min'] = min(polarities)
        sentimentData['polarity_max_sent'] = str(blob.sentences[polarities.index(sentimentData['polarity_max'])])
        sentimentData['polarity_min_sent'] = str(blob.sentences[polarities.index(sentimentData['polarity_min'])])
        sentimentData['subjectivity_avg'] = sum(subjectivities)/float(len(sentiments))
        sentimentData['subjectivity_max'] = max(subjectivities)
        sentimentData['subjectivity_min'] = min(subjectivities)
        sentimentData['subjectivity_max_sent'] = str(blob.sentences[subjectivities.index(sentimentData['subjectivity_max'])])
        sentimentData['subjectivity_min_sent'] = str(blob.sentences[subjectivities.index(sentimentData['subjectivity_min'])])
        return {doc_id: sentimentData, 'status':200}


class EntityConfirmationService():

    ''' 
    Confirms an entity for a given wiki
    Intentionally unexposed from REST endpoint (for now)
    '''
    @cachedServiceRequest
    def confirm(self, wiki_url, entities):
        '''Given a wiki URL and a group of entities,
        confirm the existence of these entities as titles via service.
        :param wiki_url: the URL of the wiki
        :param entities: a list of entities
        '''
        global MEMOIZED_ENTITIES
        memo = MEMOIZED_ENTITIES.get(wiki_url, {})
        memo_vals = memo.values()
        memo_keys = memo.keys()
        existing_entities = dict([(entity, entity) for entity in entities if entity in memo_vals] \
                               + [(entity, memo[entity]) for entity in entities if entity in memo_keys])
        
        def filterfn(current): return current not in existing_entities.keys() and current not in existing_entities.values()

        unknown_entities = filter(filterfn, entities)

        params = {
            'controller': 'WikiaSearchController',
                'method': 'resolveEntities',
              'entities': '|'.join(unknown_entities)
        }

        try:
            response = requests.get('%s/wikia.php' % wiki_url, params=params).json()
            MEMOIZED_ENTITIES[wiki_url] = dict(memo.items() + response.items())
        except KeyboardInterrupt:
            sys.exit()
        except: # sometimes a json object cannot be decoded?
            response = {}

        return {'status':200, wiki_url:dict(existing_entities.items() + response.items())}

class EntitiesService(restful.Resource):

    ''' Identifies, confirms, and counts entities over a given page '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Given an article id, accesses entities and confirms entities
        :param doc_id: the id of the article document
        '''

        wid = doc_id.split('_')[0]
        wiki_url = SolrWikiService().get(wid).get(wid)['url']

        nps = [sanitizePhrase(phrase) for phrase in AllNounPhrasesService().get(doc_id).get(doc_id)]
        if not nps:
            return {'status':400, 'message': 'Document not found'}

        confirmations = {}
        for i in range(0, len(set(nps)), 10):
            new_confirmations = EntityConfirmationService().confirm(wiki_url, nps[i:i+10]).get(wiki_url)
            confirmations = dict(confirmations.items() 
                               + [item for item in new_confirmations.items() if item[1]])

        return {doc_id:confirmations, 'status':200}

class EntityCountsService(restful.Resource):
    
    ''' Counts the entities using coreference counts in a given document '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Given a doc id, accesses entities and then cross-references entity parses 
        :param doc_id: the id of the article
        '''
        confirmed = EntitiesService().get(doc_id).get(doc_id, {})
        coreferences = CoreferenceCountsService().get(doc_id).get(doc_id, {})
        
        exists = lambda x: x is not None
        docParaphrases = coreferences.get('paraphrases', {})
        coref_mention_keys = filter(exists,  map(faultTolerantLower, docParaphrases.keys()))
        coref_mention_values = filter(exists, map(faultTolerantLower, [item for sublist in docParaphrases.values() for item in sublist]))
        paraphrases = dict([(faultTolerantLower(item[0]), filter(exists, map(faultTolerantLower, item[1])))\
                            for item in docParaphrases.items()])

        counts ={}

        for val in filter(exists, map(faultTolerantLower, confirmed.values())):
            if val in coref_mention_keys:
                counts[val] = len(paraphrases[val])
            elif val in coref_mention_values:
                for key in coref_mention_keys:
                    if val in paraphrases[key]:
                        counts[val] = len(paraphrases[key])
                        break

        return {doc_id: counts, 'status': 200}



class TopEntitiesService(restful.Resource):

    ''' Aggregates entities over a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching title.
        We then cross-reference that noun phrase by mention count. 
        :param wiki_id: the id of the wiki
        '''

        wiki = SolrWikiService().get(wiki_id).get(wiki_id, None)
        if not wiki:
            return {'status':400, 'message':'Not Found'}

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entities_to_count = {}
        entity_service = EntityCountsService()

        for page_doc_id in page_doc_response.get(wiki_id, []):
            for (entity, count) in entity_service.get(page_doc_id).get(page_doc_id, {}).items():
                entities_to_count[entity] = entities_to_count.get(entity, count)
            print entities_to_count

        counts_to_entities = {}
        for entity in entities_to_count.keys():
            cnt = entities_to_count[entity]
            counts_to_entities[cnt] = counts_to_entities.get(cnt, []) + [entity]

        return {wiki_id:counts_to_entities, 'status':200}
            


class ListDocIdsService(restful.Resource):
    
    ''' Service to expose resources in WikiDocumentIterator '''
    @cachedServiceRequest
    def get(self, wiki_id, start=0, limit=None):

        xmlPath = '%s/%s/' % (XML_PATH, wiki_id)
        if not path.exists(xmlPath):
            return {'status':500, 'message':'Wiki not yet processed'}

        if limit:
            ids = ArticleDocIdIterator(wiki_id)[start:limit]
        else:
            ids = [id for id in ArticleDocIdIterator(wiki_id)[start:]]
        return {wiki_id: ids, 'status':200, 'numFound':len(ids)}



class ArticleDocIdIterator:

    ''' Get all existing document IDs for a wiki -- not a service '''
    
    def __init__(self, wid):
        ''' Constructor method 
        :param wid: the wiki ID we want to iterate over
        '''
        self.wid = wid
        self.counter = 0
        self.files = [wid+'_'+xmlFile.split('.')[0] for xmlFiles in listdir('%s/%s' % (XML_PATH, str(wid))) \
                          for xmlFile in listdir('%s/%s/%s' % (XML_PATH, str(wid), xmlFiles))]
        self.files.sort() #why not?

    def __iter__(self):
        ''' Iterator method '''
        return self.next()

    def __getitem__(self, index):
        ''' Allows array access 
        :param index: int value of index
        '''
        return self.files[index]

    def next(self):
        ''' Get next article ID '''
        if self.counter == len(self.files):
            raise StopIteration
        self.counter += 1
        return self.files[self.counter - 1]


def sanitizePhrase(phrase):
    ''' "Sanitizes" noun phrases for better matching with article titles '''
    return re.sub(r" 's$", '', phrase)


def asList(value):
    ''' Determines if the value is a list and wraps a singleton into a list if necessary,
    done to handle the inconsistency in xml to dict
    '''
    return value if isinstance(value, types.ListType) else [value]

def faultTolerantLower(val):
    try:
        return str(val).encode('utf8').lower()
    except UnicodeEncodeError:
        pass
    except KeyboardInterrupt:
        sys.exit()
    return None

def isEmptyDoc(doc):
    ''' Lets us know if the document is empty
    :param doc: a dict object corresponding to an xml document
    '''
    return doc.get('root', {}).get('document', {}).get('sentences', None) is None

from flask.ext import restful
from text.blob import TextBlob
from nlp_client.services import *
from os import path, listdir
from gzip import open as gzopen
import nltk
import xmltodict
import requests
import re


'''
This module contains all services used in our RESTful client.
At this point, they are all read-only, and only respond to GET.
'''

XML_PATH = '/data/xml/'

# TODO: use load balancer, not a partiucular query slave
SOLR_URL = 'http://search-s10:8983'

MEMOIZED_WIKIS = {}
MEMOIZED_ENTITIES = {}

class ParsedXmlService(restful.Resource):

    ''' Read-only service responsible for accessing XML from FS '''
    
    def get(self, doc_id):
        ''' Return a response with the XML of the parsed text 
        :param doc_id: the id of the document in Solr
        '''

        response = {}
        (wid, id) = doc_id.split('_')
        # currently using flat directory
        # xmlPath = '%s/%s/%s/%s.xml' % (XML_PATH, wid, id[0], doc_id)
        xmlPath = '%s/%s/%s.xml' % (XML_PATH, wid, id)
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

    def get(self, doc_id):
        ''' Returns coreference and mentions for a document 
        :param doc_id: the id of the document in Solr
        '''
        
        response = {}
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse
        coreferences = jsonResponse[doc_id]['root']['document']['coreference']['coreference']
        sentences = jsonResponse[doc_id]['root']['document']['sentences']['sentence']
        mentionCounts = {}
        representativeToMentions = {}
        for coref in coreferences:
            mentionString = ''
            mentionCount = 0
            mentions = []
            for mention in coref['mention']:
                mentionCount += 1
                currentMentionString = " ".join([token['word'] for token in sentences[int(mention['sentence'])-1]['tokens']['token'][int(mention['start'])-1:int(mention['end'])-1]]) 
                if mention.get('@representative', 'false') == 'true':
                    mentionString = currentMentionString
                mentions += [currentMentionString]
            mentionCounts[mentionString] = mentionCount
            representativeToMentions[mentionString] = mentions
        return {doc_id: {'mentionCounts':mentionCounts, 'paraphrases': representativeToMentions}}



class AllNounPhrasesService(restful.Resource):

    ''' Read-only service that gives all noun phrases for a document
    TextBlob could do this too
    Uses ParsedJsonService
    '''
    
    def get(self, doc_id):
        ''' Get noun phrases for a document 
        :param doc_id: the id of the document in Solr
        '''
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse
        dict = jsonResponse[doc_id]
        nps = []
        for sentence in dict['root']['document']['sentences']['sentence']:
            nps += [' '.join(f.leaves()) for f in nltk.Tree.parse(sentence['parse']).subtrees() if f.node == u'NP']
        return {doc_id:nps}


class SolrPageService(restful.Resource):

    ''' Read-only service that accesses a single page-level document from Solr '''
    
    def get(self, doc_id):
        ''' Get page from solr for a document id 
        :param doc_id: the id of the document in Solr
        '''
        return {doc_id: requests.get(SOLR_URL+'/solr/main/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0]}


class SolrWikiService(restful.Resource):

    ''' Read-only service that accesses a single wiki-level document from Solr '''
    
    def get(self, doc_id):
        ''' Get wiki from solr for a document id
        :param doc_id: the id of the document in Solr
        '''
        global MEMOIZED_WIKIS
        if MEMOIZED_WIKIS.get(doc_id, None):
            return {doc_id: MEMOIZED_WIKIS[doc_id]}

        serviceResponse = {doc_id: requests.get(SOLR_URL+'/solr/xwiki/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0]}

        MEMOIZED_WIKIS = dict(MEMOIZED_WIKIS.items() + serviceResponse.items())

        return serviceResponse


class SentimentService(restful.Resource):

    ''' Read-only service that calculates the sentiment for a given piece of text
    Relies on SolrPageService
    '''
    
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
        return {doc_id: sentimentData}


class EntityConfirmationService():

    ''' 
    Confirms an entity for a given wiki
    Intentionally unexposed from REST endpoint (for now)
    '''

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
        print existing_entities, unknown_entities
        params = {
            'controller': 'WikiaSearchController',
                'method': 'resolveEntities',
              'entities': '|'.join(unknown_entities)
        }

        response = requests.get('%s/wikia.php' % wiki_url, params=params).json()
        MEMOIZED_ENTITIES[wiki_url] = dict(memo.items() + response.items())

        return dict(existing_entities.items() + response.items())

class EntitiesService(restful.Resource):

    ''' Identifies, confirms, and counts entities over a given page '''

    def get(self, doc_id):
        ''' Given an article id, accesses entities and confirms entities
        :param doc_id: the id of the article document
        '''

        wid = doc_id.split('_')[0]
        wiki_url = SolrWikiService().get(wid).get(wid)['url']

        nps = AllNounPhrasesService().get(doc_id).get(doc_id)
        if not nps:
            return {'status':400, 'message': 'Document not found'}

        confirmations = {}
        for i in range(0, len(set(nps)), 10):
            new_confirmations = EntityConfirmationService().confirm(wiki_url, nps[i:i+10])
            confirmations = dict(confirmations.items() 
                               + [item for item in new_confirmations.items() if item[1]])

        return confirmations

        

class TopEntitiesService(restful.Resource):

    ''' Aggregates entities over a wiki '''

    def get(self, doc_id):
        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching title.
        We then cross-reference that noun phrase by mention count. 
        :param doc_id: the id of the wiki
        '''

        wiki = SolrWikiService().get(doc_id).get(doc_id, None)
        if not wiki:
            return {'status':400, 'message':'Not Found'}

        
        xmlPath = '%s/%s/' % (XML_PATH, wiki['id'])
        if not path.exists(xmlPath):
            return {'status':500, 'message':'Wiki not yet processed'}

        entitiesToCount = {}
        confirm = EntityConfirmationService().get
        for file in os.listdir(xmlPath):
            pageid = file.split('.')[0]
            docid = '%s_%s' % (wiki['id'], pageid)
            corefs = CoreferencesCountService().get(docid).get(['docid'], None)
            if not corefs:
                continue
            for name in corefs['paraphrases'].keys():
                cands = set([sanitizeEntity(par) for par in corefs['paraphrases']['name']])
                print confirm(wiki['url_s'], cands)

def sanitizePhrase(phrase):
    return re.sub(r" 's$", '', phrase)
            

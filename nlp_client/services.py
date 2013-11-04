from flask.ext import restful
from text.blob import TextBlob
from os import path, listdir
from gzip import open as gzopen
from caching import cachedServiceRequest, write_only
from mrg_utils import Sentence as MrgSentence
from boto import connect_s3
from boto.s3.key import Key
import socket
import time
import title_confirmation
import re
import nltk
import xmltodict
import requests
import types
import json
import sys
import numpy

'''
This module contains all services used in our RESTful client.
At this point, they are all read-only, and only respond to GET.
'''

S3_BUCKET = None

def get_s3_bucket():
    '''
    Accesses an S3 connection for us, memoized
    :return: s3 connection
    :rtype :class:boto.s3.connection.S3Connection
    '''
    global S3_BUCKET
    if S3_BUCKET is None:
        S3_BUCKET = connect_s3().get_bucket('nlp-data')
    return S3_BUCKET


XML_PATH = '/data/xml/'

# TODO: use load balancer, not a partiucular query slave
SOLR_URL = 'http://search-s10:8983'

MEMOIZED_WIKIS = {}
MEMOIZED_JSON = {}

class RestfulResource(restful.Resource):
    
    ''' Wraps restful.Resource to allow additional logic '''

    def nestedGet(self, doc_id, backoff=None):
        ''' Allows us to call a service and extract data from its response 
        :param doc_id: the id of the document
        :param backoff: default value
        '''
        return self.get(doc_id).get(doc_id, backoff)



class ParsedXmlService(RestfulResource):

    ''' Read-only service responsible for accessing XML from FS '''
    def get(self, doc_id):
        ''' Right now just points to new s3 method, just didn't want to remove the old logic just yet.
        :param doc_id: the doc id
        '''
        return self.get_from_s3(doc_id)


    def get_from_s3(self, doc_id):
        ''' Returns a response with the XML of the parsed text
        :param doc_id: the id of the document in Solr
        '''
        try:
            bucket = get_s3_bucket()
            key = Key(bucket)
            key.key = 'xml/%s/%s.xml' % tuple(doc_id.split('_'))

            if key.exists():
                response = {'status': 200, doc_id:key.get_contents_as_string()}
            else:
                response = {'status': 500, 'message': 'Key does not exist'}
            return response
        except socket.error:
            # probably need to refresh our connection
            global S3_BUCKET
            S3_BUCKET = None
            return self.get_from_s3(doc_id)


    def get_from_file(self, doc_id):
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


class ParsedJsonService(RestfulResource):

    ''' Read-only service responsible for accessing XML and transforming it to JSON
    Uses the ParsedXmlService
    '''
    def get(self, doc_id):
        ''' Returns document parse as JSON 
        :param doc_id: the id of the document in Solr
        '''

        global MEMOIZED_JSON

        response = MEMOIZED_JSON.get(doc_id, {})

        if len(response) == 0:
            try:
                xmlResponse = ParsedXmlService().get(doc_id)
                if xmlResponse['status'] != 200:
                    return xmlResponse
                MEMOIZED_JSON[doc_id] = {'status':200, doc_id: xmltodict.parse(xmlResponse[doc_id])}
                response = MEMOIZED_JSON[doc_id]
            except Exception as e:
                return {'status': 500, 'message': str(e)}
        return response


class CoreferenceCountsService(RestfulResource):

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


class PhraseService():

    ''' Not restful, allows us to abstract out what nodes we want from a tree parse '''
    
    @staticmethod
    def get(doc_id, phrase_types):
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return []
        return PhraseService.phrases_from_json(jsonResponse[doc_id], asList(phrase_types))


    @staticmethod
    def phrases_from_json(json_parse, phrase_types):
        return [' '.join(f.leaves())
                for sentence in asList(json_parse.get('root', {}).get('document', {}).get('sentences', {}).get('sentence', []))
                for f in nltk.Tree.parse(sentence.get('parse', '')).subtrees() if f.node in phrase_types
                ] if not isEmptyDoc(json_parse) else []


class AllNounPhrasesService(RestfulResource):

    ''' Read-only service that gives all noun phrases for a document '''

    #@cachedServiceRequest
    def get(self, doc_id):
        ''' Get noun phrases for a document 
        :param doc_id: the id of the document in Solr
        '''
        return {doc_id:PhraseService.get(doc_id, [u'NP']), 'status':200}


class AllVerbPhrasesService(RestfulResource):

    ''' Read-only service that gives all verb phrases for a document '''

    @cachedServiceRequest
    def get(self, doc_id):
        ''' Get verb phrases for a document 
        :param doc_id: the id of the document in Solr
        '''
        return {doc_id:PhraseService.get(doc_id, [u'VP']), 'status':200}


class HeadsService(RestfulResource):

    ''' Provides syntactic heads for a given document '''

    @cachedServiceRequest
    def get(self, doc_id):
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse
        dict = jsonResponse[doc_id]
        counter = 0
        if not isEmptyDoc(dict):
            return {'status':200,
                    doc_id: [title_confirmation.preprocess(MrgSentence(sentence.get('parse', '')).nodes.getTermHead().getString()) \
                                 for sentence in asList(dict.get('root', {}).get('document', {}).get('sentences', {}).get('sentence', [])) \
                                 ]
                    }
        else:
            return {'status':400,
                    'message': "No sentences found"}
            

class HeadsCountService(RestfulResource):

    ''' Provides a count for all heads in a wiki '''

    @cachedServiceRequest
    def get(self, wiki_id):
        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        page_doc_ids = page_doc_response.get(wiki_id, [])
        hs = HeadsService()
        allHeads = [head for heads in filter(lambda x: x is not None, map(hs.nestedGet, page_doc_ids)) for head in heads]
        singleHeads = set(allHeads)
        return {'status':200, wiki_id: dict(zip(singleHeads, map(allHeads.count, singleHeads))) }


class TopHeadsService(RestfulResource):

    ''' Gets the most frequent syntactic in a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):
        heads_to_counts = HeadsCountService().nestedGet(wiki_id, {})
        items = sorted(heads_to_counts.items(), \
                           key=lambda item:int(item[1]), \
                           reverse=True)

        return {'status': 200, wiki_id: items}


class SolrPageService(RestfulResource):

    ''' Read-only service that accesses a single page-level document from Solr '''
    
    def get(self, doc_id):
        ''' Get page from solr for a document id 
        :param doc_id: the id of the document in Solr
        '''
        return {doc_id: requests.get(SOLR_URL+'/solr/main/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0], 'status':200}


class SolrWikiService(RestfulResource):

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


class NaiveSentimentService(RestfulResource):

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
        return {doc_id: sentimentData, 'status': 200}

class DocumentSentimentService(RestfulResource):

    ''' Responsible for delivering sentiment information for a given document.
    '''

    @cachedServiceRequest
    def get(self, doc_id):
        '''
        Provides average sentiment across the document, and sentiment scores for entities within each subject.
        :return: dictionary with response data
        :rtype:dict
        '''
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse.get('status', None) is not 200:
            return jsonResponse

        doc = jsonResponse.get(doc_id, {})

        response = dict()

        sentencesNode = doc.get('root', {}).get('document', {}).get('sentences', {})

        if sentencesNode is None:
            response['status'] = 500
            response['message'] = 'No sentences in this parse.'
            return response

        response['averageSentiment'] = sentencesNode.get('averageSentiment')

        if response['averageSentiment'] is None:
            response['status'] = 500
            response['message'] = 'Sentiment data missing from parse.'
            return response

        docParaphrases = CoreferenceCountsService().nestedGet(doc_id, {}).get('paraphrases', {})

        val_to_canonical = dict(map(lambda x: map(title_confirmation.preprocess, x),
                                [(key, key) for key in docParaphrases]
                                + [(value, key) for key in docParaphrases for value in docParaphrases[key]]))

        if '' in val_to_canonical:
            del val_to_canonical['']  # wat

        phrasesToSentiment = dict()

        def traverse_tree_for_sentiment(sent, parse=None):
            try:
                if parse is None:
                    sexpr = sent.get('sentence', {}).get('parse', '').decode('ISO-8859-2').encode('utf-8')
                    if sexpr == '':
                        return  # can't do anything with this junk
                    parse = nltk.Tree.parse(sexpr)

                flattened = str(parse.flatten()) if not isinstance(parse, basestring) else parse
                if flattened in val_to_canonical:
                    phrasesToSentiment[flattened] = phrasesToSentiment.get(flattened, []) + [int(sent['@sentiment'])]
                    return  # don't need to keep going

                if not isinstance(parse, basestring):
                    for i in range(0, len(parse)):
                        traverse_tree_for_sentiment(sent, parse[i])
            except UnicodeEncodeError:
                return  # gotta fix

        sentences = doc.get('root', {}).get('document', {}).get('sentences', {}).get('sentence')
        if isinstance(sentences, dict):
            #singleton sentence
            sentences = [sentences]

        map(traverse_tree_for_sentiment, sentences)

        response['averagePhraseSentiment'] = dict([(x[0], numpy.mean(x[1])) for x in phrasesToSentiment.items()])

        return {'status': 200, doc_id: response}


class DocumentEntitySentimentService(RestfulResource):

    ''' Filters out sentiment in a document to only care about entities '''
    @cachedServiceRequest
    def get(self, doc_id):
        sentimentResponse = DocumentSentimentService().get(doc_id)

        if sentimentResponse['status'] is not 200:
            return sentimentResponse

        entities = EntitiesService().nestedGet(doc_id, [])

        return {'status': 200,
                'entities': dict(filter(lambda x: title_confirmation.check_wp(x[0]) or x[0] in entities,
                                        sentimentResponse[doc_id]['averagePhraseSentiment'].items()))
                }

class WikiEntitySentimentService(RestfulResource):

    ''' Does document entity sentiment service across all documents '''
    @cachedServiceRequest
    def get(self, wiki_id):

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entitySentiment = {}
        dss = DocumentSentimentService()
        for doc_id in page_doc_response[wiki_id]:
            sent_response = dss.getNested(wiki_id)
            for key in sent_response:
                entitySentiment[key] = entitySentiment.get(key, []) + sent_response[key]

        return {'status': 200, wiki_id: dict([(key, numpy.mean(entitySentiment[key])) for key in entitySentiment])}


class WpWikiEntitySentimentService(RestfulResource):

    ''' Does document entity sentiment service across all documents '''
    @cachedServiceRequest
    def get(self, wiki_id):

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entitySentiment = {}
        dss = WpDocumentSentimentService()
        for doc_id in page_doc_response[wiki_id]:
            sent_response = dss.getNested(wiki_id)
            for key in sent_response:
                entitySentiment[key] = entitySentiment.get(key, []) + sent_response[key]

        return {'status': 200, wiki_id: dict([(key, numpy.mean(entitySentiment[key])) for key in entitySentiment])}

        

class WpDocumentEntitySentimentService(RestfulResource):

    ''' Filters out sentiment in a document to only care about entities '''
    @cachedServiceRequest
    def get(self, doc_id):
        sentimentResponse = DocumentSentimentService().get(doc_id)
        if sentimentResponse['status'] is not 200:
            return sentimentResponse

        entities = WpEntitiesService().nestedGet(doc_id, [])

        return {'status': 200,
                'entities': dict(filter(lambda x: title_confirmation.check_wp(x[0]) or x[0] in entities,
                                        sentimentResponse[doc_id]['averagePhraseSentiment'].items()))
                }

class AllEntitiesSentimentAndCountsService(RestfulResource):

    ''' Key is entity name, and then dict of count and sentiment so we can sort and what not '''
    @cachedServiceRequest
    def get(self, wiki_id):
        counts = dict(
            WpDocumentEntityCountsService().nestedGet(wiki_id).items() +
            DocumentEntityCountsService().nestedGet(wiki_id).items()
        )
        sentiments = dict (
            DocumentEntitySentimentService().nestedGet(wiki_id) +
            WpDocumentEntitySentimentService().nestedGet(wiki_id)
        )

        resp_dict = {}
        for s in sentiments:
            resp_dict[s] = {'sentiment': sentiments[s] }

        for c in counts:
            respDict[c] = dict(resp_dict.get(c, {}).items() + ('count', counts[c]))
        
        return { 'status': 200, wiki_id: resp_dict }


class AllTitlesService(RestfulResource):

    ''' Responsible for accessing all titles from database using title_confirmation module '''
    @cachedServiceRequest
    def get(self, wiki_id):
        ''' Extracts titles for a wiki from database
        The module it uses stores this value memory when caching is off.
        :param wiki_id: the id of the wiki
        '''
        return {'status': 200, wiki_id: list(title_confirmation.get_titles_for_wiki_id(wiki_id))}
        

class RedirectsService(RestfulResource):

    ''' Responsible for accessing list of redirects, correlating to their canonical title '''
    @cachedServiceRequest
    def get(self, wiki_id):
        ''' Gives us a dictionary of redirect to canonical title
        In-memory caching when we don't have db caching.
        :param wiki_id: the id of the wiki
        '''
        return {'status': 200, wiki_id: title_confirmation.get_redirects_for_wiki_id(wiki_id)}


class WpEntitiesService(RestfulResource):

    ''' Identifies, confirms, and counts entities matching wikipedia titles over a given page '''
    @cachedServiceRequest
    def get(self, doc_id):
        """
        Cross-references each noun phrase with wikipedia
        :param doc_id: the id of the document
        """
        nps = AllNounPhrasesService().get(doc_id).get(doc_id, [])
        nps_filtered = filter(lambda x: len(x.strip()) > 0,  filter(title_confirmation.check_wp, filter(lambda x: len(x.split(' ')) <= 5,  list(set(nps)))))
        return {'status':200, doc_id: [np for np in nps if np in nps_filtered] }

class EntitiesService(RestfulResource):

    ''' Identifies, confirms, and counts entities over a given page '''
    @cachedServiceRequest
    def get(self, doc_id):
        """
        Use title_confirmation module to make this fast on a per-wiki basis
        :param doc_id: the id of the document
        """
        resp = {'status':200}

        nps = AllNounPhrasesService().get(doc_id).get(doc_id, [])

        titles = AllTitlesService().nestedGet(doc_id.split('_')[0])
        redirects = RedirectsService().nestedGet(doc_id.split('_')[0])

        if nps is None:
            return {'status': 200, doc_id:{'titles': [], 'redirects': {}}}

        checked_titles = filter(lambda x: x in titles, map(title_confirmation.preprocess, nps))

        resp['titles'] = list(set(checked_titles))

        resp['redirects'] = dict(filter(lambda x: x[1], map(lambda x: (x, redirects.get(x, None)), checked_titles)))

        return {'status':200, doc_id:resp}


class EntityCountsService(RestfulResource):
    
    ''' Counts the entities using coreference counts in a given document '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Given a doc id, accesses entities and then cross-references entity parses 
        :param doc_id: the id of the article
        '''
        entitiesresponse = EntitiesService().get(doc_id).get(doc_id, {})
        coreferences = CoreferenceCountsService().get(doc_id).get(doc_id, {})
        
        exists = lambda x: x is not None
        docParaphrases = coreferences.get('paraphrases', {})
        coref_mention_keys = map(title_confirmation.preprocess, docParaphrases.keys())
        coref_mention_values = map(title_confirmation.preprocess, [item for sublist in docParaphrases.values() for item in sublist])
        paraphrases = dict([(title_confirmation.preprocess(item[0]), map(title_confirmation.preprocess, item[1]))\
                            for item in docParaphrases.items()])

        counts ={}

        for val in entitiesresponse['titles']:
            try:
                canonical = entitiesresponse['redirects'].get(val, val)
                if canonical in coref_mention_keys:
                    counts[canonical] = len(paraphrases[canonical])
                elif canonical != val and val in coref_mention_keys:
                    counts[canonical] = len(paraphrases[val])
                elif canonical in coref_mention_values:
                    counts[canonical] = len(filter(lambda x: canonical in x[1], paraphrases.items())[0][1])
                elif canonical != val and val in coref_mention_values:
                    counts[canonical] = len(filter(lambda x: val in x[1], paraphrases.items())[0][1])
            except Exception as e:
                print e.message

        return {doc_id: counts, 'status': 200}


class WpEntityCountsService(RestfulResource):
    
    ''' Counts the wikipedia entities using coreference counts in a given document '''
    @cachedServiceRequest
    def get(self, doc_id):
        ''' Given a doc id, accesses wp entities and then cross-references entity parses 
        :param doc_id: the id of the article
        '''

        entities = WpEntitiesService().nestedGet(doc_id)
        coreferences = CoreferenceCountsService().get(doc_id).get(doc_id, {})
        
        exists = lambda x: x is not None
        docParaphrases = coreferences.get('paraphrases', {})
        coref_mention_keys = map(title_confirmation.preprocess, docParaphrases.keys())
        coref_mention_values = map(title_confirmation.preprocess, [item for sublist in docParaphrases.values() for item in sublist])
        paraphrases = dict([(title_confirmation.preprocess(item[0]), map(title_confirmation.preprocess, item[1]))\
                            for item in docParaphrases.items()])

        counts ={}

        for val in map(title_confirmation.preprocess, entities):
            try:
                canonical = val
                if canonical in coref_mention_keys:
                    counts[canonical] = len(paraphrases[canonical])
                elif canonical != val and val in coref_mention_keys:
                    counts[canonical] = len(paraphrases[val])
                elif canonical in coref_mention_values:
                    counts[canonical] = len(filter(lambda x: canonical in x[1], paraphrases.items())[0][1])
                elif canonical != val and val in coref_mention_values:
                    counts[canonical] = len(filter(lambda x: val in x[1], paraphrases.items())[0][1])
            except:
                pass

        return {doc_id: counts, 'status': 200}


class TopEntitiesService(RestfulResource):
    
    ''' Gets the most frequent entities in a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):
        counts_to_entities = WikiEntitiesService().get(wiki_id).get(wiki_id, {})
        items = sorted([(val, key) for key in counts_to_entities.keys() for val in counts_to_entities[key]], \
                           key=lambda item:int(item[1]), \
                           reverse=True)

        return {'status': 200, wiki_id: items[:50]}

class WpTopEntitiesService(RestfulResource):

    ''' Gets most frequent wikipedia entities in a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):
        counts_to_entities = WpWikiEntitiesService().get(wiki_id).get(wiki_id, {})
        items = sorted([(val, key) for key in counts_to_entities.keys() for val in counts_to_entities[key]], \
                           key=lambda item:int(item[1]), \
                           reverse=True)

        return {'status': 200, wiki_id: items[:50]}


class WikiEntitiesService(RestfulResource):

    ''' Aggregates entities over a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching title.
        We then cross-reference that noun phrase by mention count. 
        :param wiki_id: the id of the wiki
        '''

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entities_to_count = {}
        entity_service = EntityCountsService()

        counter = 1
        page_doc_ids = page_doc_response.get(wiki_id, [])
        total = len(page_doc_ids)
        for page_doc_id in page_doc_ids:
            entities_with_count = entity_service.get(page_doc_id).get(page_doc_id, {}).items()
            map(lambda x: entities_to_count.__setitem__(x[0], entities_to_count.get(x[0], 0) + x[1]) , entities_with_count)
            print '(%s/%s)' % (counter,total)
            counter += 1

        counts_to_entities = {}
        for entity in entities_to_count.keys():
            cnt = entities_to_count[entity]
            counts_to_entities[cnt] = counts_to_entities.get(cnt, []) + [entity]

        return {wiki_id:counts_to_entities, 'status':200}

class WikiPageEntitiesService(RestfulResource):

    ''' Aggregates entities over a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching title.
        We then cross-reference that noun phrase by mention count. 
        :param wiki_id: the id of the wiki
        '''

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entity_service = EntityCountsService()

        return {'status': 200, wiki_id: dict([(page_doc_id, entity_service.nestedGet(page_doc_id)) for page_doc_id in page_doc_response.get(wiki_id, [])])}


class WpWikiPageEntitiesService(RestfulResource):

    ''' Aggregates wp entities over a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching wp title.
        We then cross-reference that noun phrase by mention count. 
        :param wiki_id: the id of the wiki
        '''

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entity_service = WpEntityCountsService()

        return {'status': 200, wiki_id: dict([(page_doc_id, entity_service.nestedGet(page_doc_id)) for page_doc_id in page_doc_response.get(wiki_id, [])])}


class WpWikiEntitiesService(RestfulResource):
    ''' Entities service, but for Wikipedia '''

    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching title.
        We then cross-reference that noun phrase by mention count. 
        :param wiki_id: the id of the wiki
        '''

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entities_to_count = {}
        entity_service = WpEntityCountsService()

        counter = 1
        page_doc_ids = page_doc_response.get(wiki_id, [])
        total = len(page_doc_ids)
        for page_doc_id in page_doc_ids:
            entities_with_count = entity_service.get(page_doc_id).get(page_doc_id, {}).items()
            map(lambda x: entities_to_count.__setitem__(x[0], entities_to_count.get(x[0], 0) + x[1]) , entities_with_count)
            print '(%s/%s)' % (counter,total)
            counter += 1

        counts_to_entities = {}
        for entity in entities_to_count.keys():
            cnt = entities_to_count[entity]
            counts_to_entities[cnt] = counts_to_entities.get(cnt, []) + [entity]

        return {wiki_id:counts_to_entities, 'status':200}


class EntityDocumentCountsService(RestfulResource):
    
    ''' Counts the number of documents each entity appears in '''
    ''' Aggregates entities over a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching title.
        We then cross-reference that noun phrase by document count, not mention count
        :param wiki_id: the id of the wiki
        '''

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entities_to_count = {}
        entity_service = EntityCountsService()

        counter = 1
        page_doc_ids = page_doc_response.get(wiki_id, [])
        total = len(page_doc_ids)
        print total
        for page_doc_id in page_doc_ids:
            entities_with_count = entity_service.get(page_doc_id).get(page_doc_id, {}).items()
            map(lambda x: entities_to_count.__setitem__(x[0], entities_to_count.get(x[0], 0) + 1) , entities_with_count)
            counter += 1
            print "%d / %d" % (counter, total)

        counts_to_entities = {}
        for entity in entities_to_count.keys():
            cnt = entities_to_count[entity]
            counts_to_entities[cnt] = counts_to_entities.get(cnt, []) + [entity]

        return {wiki_id:counts_to_entities, 'status':200}

class WpEntityDocumentCountsService(RestfulResource):
    
    ''' Counts the number of documents each wikipedia entity appears in '''
    ''' Aggregates entities over a wiki '''
    @cachedServiceRequest
    def get(self, wiki_id):

        ''' Given a wiki doc id, iterates over all documents available.
        For each noun phrase, we confirm whether there is a matching wp title.
        We then cross-reference that noun phrase by document count, not mention count
        :param wiki_id: the id of the wiki
        '''

        page_doc_response = ListDocIdsService().get(wiki_id)
        if page_doc_response['status'] != 200:
            return page_doc_response

        entities_to_count = {}
        entity_service = WpEntityCountsService()

        counter = 1
        page_doc_ids = page_doc_response.get(wiki_id, [])
        total = len(page_doc_ids)
        for page_doc_id in page_doc_ids:
            entities_with_count = entity_service.get(page_doc_id).get(page_doc_id, {}).items()
            map(lambda x: entities_to_count.__setitem__(x[0], entities_to_count.get(x[0], 0) + 1) , entities_with_count)
            counter += 1

        counts_to_entities = {}
        for entity in entities_to_count.keys():
            cnt = entities_to_count[entity]
            counts_to_entities[cnt] = counts_to_entities.get(cnt, []) + [entity]

        return {wiki_id:counts_to_entities, 'status':200}
            

class ListDocIdsService(RestfulResource):
    
    ''' Service to expose resources in WikiDocumentIterator '''
    @cachedServiceRequest
    def get(self, wiki_id, start=0, limit=None):

        bucket = get_s3_bucket()
        keys = bucket.get_all_keys(prefix='xml/%s/' % (str(wiki_id)), max_keys=1)
        if len(keys) == 0:
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
        bucket = get_s3_bucket()
        self.wid = wid
        self.counter = 0
        def id_from_key(x):
            split = x.split('/')
            return "%s_%s" % (split[-2], split[-1].replace('.xml', ''))
        self.keys = [id_from_key(key.key) for key in bucket.list(prefix='xml/'+str(wid)+'/') if key.key.endswith('.xml')]

    def __iter__(self):
        ''' Iterator method '''
        return self.next()

    def __getitem__(self, index):
        ''' Allows array access 
        :param index: int value of index
        '''
        return self.keys[index]

    def next(self):
        ''' Get next article ID '''
        if self.counter == len(self.keys):
            raise StopIteration
        self.counter += 1
        return self.keys[self.counter-1]
        



def sanitizePhrase(phrase):
    ''' "Sanitizes" noun phrases for better matching with article titles '''
    return re.sub(r" 's$", '', phrase)


def asList(value):
    ''' Determines if the value is a list and wraps a singleton into a list if necessary,
    done to handle the inconsistency in xml to dict
    '''
    return value if isinstance(value, types.ListType) else [value]

def isEmptyDoc(doc):
    ''' Lets us know if the document is empty
    :param doc: a dict object corresponding to an xml document
    '''
    return doc.get('root', {}).get('document', {}).get('sentences', None) is None

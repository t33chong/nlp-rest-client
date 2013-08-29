from flask.ext import restful
from text.blob import TextBlob
from nlp_client.services import *
from os import path
from gzip import open as gzopen
import nltk
import xmltodict
import requests


"""
This module contains all services used in our RESTful client.
At this point, they are all read-only, and only respond to GET.
"""

XML_PATH = '/data/xml/'

# TODO: use load balancer, not a partiucular query slave
SOLR_URL = 'http://search-s10:8983'

"""
Read-only service responsible for accessing XML from FS
"""
class ParsedXmlService(restful.Resource):
    """ Return a response with the XML of the parsed text """
    def get(self, doc_id):
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

"""
Read-only service responsible for accessing XML and transforming it to JSON
Uses the ParsedXmlService
"""
class ParsedJsonService(restful.Resource):
    """ Returns document parse as JSON """
    def get(self, doc_id):
        response = {}
        xmlResponse = ParsedXmlService().get(doc_id)
        if xmlResponse['status'] != 200:
            return xmlResponse
        return {'status':200, doc_id: xmltodict.parse(xmlResponse[doc_id])}

"""
Read-only service responsible for providing data on mention coreference
Uses the ParsedJsonService
"""
class CoreferenceCountsService(restful.Resource):
    """ Returns coreference and mentions for a document """
    def get(self, doc_id):
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


"""
Demo read-only service that gives all noun phrases for a document
TextBlob could do this too
Uses ParsedJsonService
"""
class AllNounPhrasesDemoService(restful.Resource):
    """ Get noun phrases for a document """
    def get(self, doc_id):
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse
        dict = jsonResponse[doc_id]
        nps = []
        for sentence in dict['root']['document']['sentences']['sentence']:
            nps += [' '.join(f.leaves()) for f in nltk.Tree.parse(sentence['parse']).subtrees() if f.node == u'NP']
        return {doc_id:nps}

"""
Read-only service that accesses a single page-level document from Solr
"""
class SolrPageService(restful.Resource):
    """ Get page from solr for a document id """
    def get(self, doc_id):
        return {doc_id: requests.get(SOLR_URL+'/solr/main/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0]}

"""
Read-only service that accesses a single wiki-level document from Solr
"""
class SolrWikiService(restful.Resource):
    """ Get wiki from solr for a document id """
    def get(self, doc_id):
        return {doc_id: requests.get(SOLR_URL+'/solr/xwiki/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0]}

"""
Read-only service that calculates the sentiment for a given piece of text
Relies on SolrPageService
"""
class SentimentService(restful.Resource):
    """ For a document id, get data on the text's polarity and subjectivity """
    def get(self, doc_id):
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

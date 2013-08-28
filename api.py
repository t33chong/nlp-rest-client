from flask import Flask
from flask.ext import restful
from os import path
from gzip import open
from text.blob import TextBlob
import nltk
import xmltodict
import requests


app = Flask(__name__)
api = restful.Api(app)

XML_PATH = '/data/nlp/'

# TODO: use load balancer, not a partiucular query slave
SOLR_URL = 'http://search-s10:8983'

class ParsedXmlService(restful.Resource):
    def get(self, doc_id):
        response = {}
        (wid, id) = doc_id.split('_')
        xmlPath = '%s/%s/%s/%s.xml.gz' % (XML_PATH, wid, id[0], doc_id)
        if path.exists(xmlPath):
            response['status'] = 200
            response[doc_id] = ''.join(open(xmlPath).readlines())
        else:
            response['status'] = 500
            response['message'] = 'File not found for document %s' % doc_id
        return response

class ParsedJsonService(restful.Resource):
    def get(self, doc_id):
        response = {}
        xmlResponse = ParsedXmlService().get(doc_id)
        if xmlResponse['status'] != 200:
            return xmlResponse
        return {'status':200, doc_id: xmltodict.parse(xmlResponse[doc_id])}

class CoreferenceCounts(restful.Resource):
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

class AllNounPhrasesDemo(restful.Resource):
    def get(self, doc_id):
        jsonResponse = ParsedJsonService().get(doc_id)
        if jsonResponse['status'] != 200:
            return jsonResponse
        dict = jsonResponse[doc_id]
        nps = []
        for sentence in dict['root']['document']['sentences']['sentence']:
            nps += [' '.join(f.leaves()) for f in nltk.Tree.parse(sentence['parse']).subtrees() if f.node == u'NP']
        return {doc_id:nps}

class SolrPage(restful.Resource):
    def get(self, doc_id):
        return {doc_id: requests.get(SOLR_URL+'/solr/main/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0]}

class SolrWiki(restful.Resource):
    def get(self, doc_id):
        return {doc_id: requests.get(SOLR_URL+'/solr/xwiki/select/', params={'q':'id:%s' % doc_id, 'wt':'json'}
).json().get('response', {}).get('docs',[None])[0]}

class Sentiment(restful.Resource):
    def get(self, doc_id):
        blob = TextBlob(SolrPage().get(doc_id).get(doc_id, {}).get('html_en', ''))
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
        

api.add_resource(ParsedXmlService, '/doc/<string:doc_id>/xml')
api.add_resource(ParsedJsonService, '/doc/<string:doc_id>/json')
api.add_resource(AllNounPhrasesDemo, '/doc/<string:doc_id>/nps')
api.add_resource(CoreferenceCounts, '/doc/<string:doc_id>/corefs')
api.add_resource(SolrPage, '/doc/<string:doc_id>/solr')
api.add_resource(Sentiment, '/doc/<string:doc_id>/sentiment')
api.add_resource(SolrWiki, '/wiki/<string:doc_id>/solr')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

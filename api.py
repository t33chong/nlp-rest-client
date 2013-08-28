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

class AllNounPhrasesDemo(restful.Resource):
    def get(self, doc_id):
        xmlResponse = ParsedXmlService().get(doc_id)
        if xmlResponse['status'] != 200:
            return xmlResponse
        dict = xmltodict.parse(xmlResponse[doc_id])
        nps = []
        for sentence in dict['root']['document']['sentences']['sentence']:
            nps += [' '.join(f.leaves()) for f in nltk.Tree.parse(sentence['parse']).subtrees() if f.node == u'NP']
        return {doc_id:nps}

class SolrPage(restful.Resource):
    def get(self, doc_id):
        return {doc_id: requests.get('http://search:8983/solr/main/select/', params={'q':'id:%s' % doc_id}
).json().get('response', {}).get('docs',[None])[0]}

class SolrWiki(restful.Resource):
    def get(self, doc_id):
        return {doc_id: requests.get('http://search:8983/solr/xwiki/select/', params={'q':'id:%s' % doc_id}
).json().get('response', {}).get('docs',[None])[0]}

class Sentiment(restful.Resource):
    def get(self, doc_id):
        blob = TextBlob(SolrPage().get(doc_id)['html_en'])
        sentiments = [s.sentiment for s in blob.sentences]
        polarity = sum([s[0] for s in sentiments])
        subjectivity = sum([s[1] for s in sentiments])
        return {doc_id: { 'polarity':polarity, 'subjectivity':subjectivity}}
        

api.add_resource(ParsedXmlService, '/doc/<string:doc_id>/xml')
api.add_resource(AllNounPhrasesDemo, '/doc/<string:doc_id>/nps')
api.add_resource(SolrPage, '/doc/<string:doc_id>/solr')
api.add_resource(Sentiment, '/doc/<string:doc_id>/sentiment')
api.add_resource(SolrWiki, '/wiki/<string:doc_id>/solr')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

from flask import Flask
from flask.ext import restful
from os import path
from gzip import open
import nltk, xmltodict

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

api.add_resource(ParsedXmlService, '/doc/<string:doc_id>/xml')
api.add_resource(AllNounPhrasesDemo, '/doc/<string:doc_id>/nps')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

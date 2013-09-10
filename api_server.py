from flask import Flask
from flask.ext import restful
from nlp_client.services import *
import sys

app = Flask(__name__)
api = restful.Api(app)

api.add_resource(ParsedXmlService,          '/doc/<string:doc_id>/xml')
api.add_resource(ParsedJsonService,         '/doc/<string:doc_id>/json')
api.add_resource(AllNounPhrasesService,     '/doc/<string:doc_id>/nps')
api.add_resource(CoreferenceCountsService,  '/doc/<string:doc_id>/corefs')
api.add_resource(SolrPageService,           '/doc/<string:doc_id>/solr')
api.add_resource(SentimentService,          '/doc/<string:doc_id>/sentiment')
api.add_resource(EntitiesService,           '/doc/<string:doc_id>/entities')
api.add_resource(EntityCountsService,       '/doc/<string:doc_id>/entity_counts')
api.add_resource(SolrWikiService,           '/wiki/<string:wiki_id>/solr')
api.add_resource(WikiEntitiesService,       '/wiki/<string:wiki_id>/entities')
api.add_resource(ListDocIdsService,         '/wiki/<string:wiki_id>/docs/') #todo: get start & offset working
api.add_resource(TopEntitiesService,        '/wiki/<string:wiki_id>/top_entities')

if __name__ == '__main__':
    if len(sys.argv) > 1:
        useCaching()
    app.run(debug=True, host='0.0.0.0')

import os
import xmltodict
import nltk

from services import PhraseService, ParsedJsonService

NOUN_TAGS = ['NP', 'NN', 'NNS', 'NNP', 'NNPS']


def field_for_wiki(wid, field, default=None):
    path = '/data/wiki_xml/%s/%s.xml' % (wid, field)
    if not os.path.exists(path):
        return default

    return open(path, 'r').read()


def phrases_for_wiki_field(wid, field):
    return PhraseService.phrases_from_json(field_for_wiki(wid, field, {}), NOUN_TAGS)


def get_main_page_nps(wid):
    doc_id = requests.get('http://search-s10:8983/solr/main/select', 
                        params=dict(wt='json', q='wid:%s AND is_main_page=true', fl='id'))\
                        .json().get('response', {}).get('docs', [{}]).get[0].get('id', None)

    return PhraseService.phrases_from_json(ParsedJsonService().nestedGet(doc_id, {}), NOUN_TAGS) if doc_id is not None else []

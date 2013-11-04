import os
import xmltodict
import nltk
import requests

from services import PhraseService, ParsedJsonService

NOUN_TAGS = ['NP', 'NN', 'NNS', 'NNP', 'NNPS']


def field_for_wiki(wid, field, default=None):
    path = '/data/wiki_xml/%s/%s.xml' % (wid, field)
    if not os.path.exists(path):
        return default

    text = open(path, 'r').read()
    if len(text) > 0:
        return xmltodict.parse(text)

    return default


def phrases_for_wiki_field(wid, field):
    return PhraseService.phrases_from_json(field_for_wiki(wid, field, {}), NOUN_TAGS)


def main_page_nps(wid):
    response = requests.get('http://search-s10:8983/solr/main/select', 
                        params=dict(wt='json', q='wid:%s AND is_main_page:true' % wid, fl='id'))

    doc_id = response.json().get('response', {}).get('docs', [{}])[0].get('id', None)

    return PhraseService.phrases_from_json(ParsedJsonService().nestedGet(doc_id, {}), NOUN_TAGS) if doc_id is not None else []

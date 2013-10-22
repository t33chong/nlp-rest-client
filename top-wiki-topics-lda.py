import json
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)
from math import sqrt
import gensim
from sklearn.svm import SVC
import os
from nlp_client.services import WikiPageEntitiesService, WikiEntitiesService, WpWikiPageEntitiesService, TopEntitiesService, HeadsCountService
from nlp_client.caching import useCaching
import sys
import requests

topN = sys.argv[1]

num_topics = int(sys.argv[2])

useCaching(perServiceCaching={'TopEntitiesService.get': {'dont_compute':True}, 'HeadsCountService.get': {'dont_compute':True}})

wids = [str(int(line)) for line in open('topwams.txt').readlines()][:int(topN)]


def vec2dense(vec, num_terms):

    '''Convert from sparse gensim format to dense list of numbers'''
    return list(gensim.matutils.corpus2dense([vec], num_terms=num_terms).T[0])

entities = []
entities = dict([(wid, [HeadsCountService().nestedGet(wid), TopEntitiesService().nestedGet(wid)]) for wid in wids])

widToEntityList = {}
for wid in entities:
    widToEntityList[wid] = []
    for entity in entities[wid][0]:
        widToEntityList[wid] += [entity] * int(entities[wid][0][entity])
    for entity in entities[wid][1]:
        widToEntityList[wid] += [entity[0]] * int(entity[1])

print len(widToEntityList)

print "Extracting..."

dct = gensim.corpora.Dictionary(widToEntityList.values())
unfiltered = dct.token2id.keys()
dct.filter_extremes(no_below=2)
filtered = dct.token2id.keys()
filtered_out = set(unfiltered) - set(filtered)
#print "\nThe following super common/rare words were filtered out..."
#print list(filtered_out), '\n'
#print "Vocabulary after filtering..."
#print dct.token2id.keys(), '\n'

print "---Bag of Words Corpus---"
 
bow_docs = {}
for name in widToEntityList:
 
    sparse = dct.doc2bow(widToEntityList[name])
    bow_docs[name] = sparse
    dense = vec2dense(sparse, num_terms=len(dct))
    #print name, ":", dense

print "\n---LDA Model---"
lda_docs = {}

lda_model = gensim.models.LdaModel(bow_docs.values(),
                                   num_topics=num_topics,
                                   id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]))

print lda_model
print lda_model.print_topics(num_topics)
lda_model.save('lda-%swikis-%stopics.model' % (sys.argv[1], sys.argv[2]))
sys.exit()
for name in widToEntityList:
    vec = bow_docs[name]
    sparse = lda_model[vec]
    dense = vec2dense(sparse, num_topics)
    lda_docs[name] = sparse
    #print name, ':', dense
    

print "\n---Unit Vectorization---"
 
unit_vecs = {}
for name in widToEntityList:
    vec = vec2dense(lda_docs[name], num_topics)
    norm = sqrt(sum(num ** 2 for num in vec))
    unit_vec = [num / norm for num in vec]
    unit_vecs[name] = unit_vec
    #print name, ':', unit_vec


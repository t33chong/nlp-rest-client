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


dct = gensim.corpora.Dictionary(widToEntityList.values())
unfiltered = dct.token2id.keys()
dct.filter_extremes(no_below=2)
filtered = dct.token2id.keys()
filtered_out = set(unfiltered) - set(filtered)
print "\nThe following super common/rare words were filtered out..."
print list(filtered_out), '\n'
#print "Vocabulary after filtering..."
#print dct.token2id.keys(), '\n'

print "---Bag of Words Corpus---"
 
bow_docs = {}
for name in widToEntityList:
 
    sparse = dct.doc2bow(widToEntityList[name])
    bow_docs[name] = sparse
    dense = vec2dense(sparse, num_terms=len(dct))
    #print name, ":", dense

print "\n---LSI Model---"
lsi_docs = {}

lsi_model = gensim.models.LsiModel(bow_docs.values(),
                                       num_topics=num_topics)
for name in widToEntityList:
    vec = bow_docs[name]
    sparse = lsi_model[vec]
    dense = vec2dense(sparse, num_topics)
    lsi_docs[name] = sparse
    #print name, ':', dense
    

print "\n---Unit Vectorization---"
 
unit_vecs = {}
for name in widToEntityList:
    vec = vec2dense(lsi_docs[name], num_topics)
    norm = sqrt(sum(num ** 2 for num in vec))
    unit_vec = [num / norm for num in vec]
    unit_vecs[name] = unit_vec
    #print name, ':', unit_vec


print "\n---Document Similarities---"


titles = json.loads(''.join(open('wikititles.json').readlines()))

index = gensim.similarities.MatrixSimilarity(lsi_docs.values())
for i, name in enumerate(widToEntityList): 
    vec = lsi_docs[name]
    sims = index[vec]
    sims = sorted(enumerate(sims), key=lambda item: -item[1])
    
    #Similarities are a list of tuples of the form (doc #, score)
    #In order to extract the doc # we take first value in the tuple
    #Doc # is stored in tuple as numpy format, must cast to int 
    if int(sims[0][0]) != i:
        match = int(sims[0][0])
    else:
        match = int(sims[1][0])
 
    match = widToEntityList.keys()[match]
    try:
        print titles.get(name, name), "is most similar to...", titles.get(match, match)
    except Exception as e:
        print e.message


"""
print "\n---Classification---"
 


dog1 = unit_vecs['dog1.txt']
sandwich1 = unit_vecs['sandwich1.txt']
 
train = [dog1, sandwich1]
 
# The label '1' represents the 'dog' category
# The label '2' represents the 'sandwich' category

label_to_name = dict([(1, 'dogs'), (2, 'sandwiches')])
labels = [1, 2]
classifier = SVC()
classifier.fit(train, labels)

for name in names:
 
    vec = unit_vecs[name]
    label = classifier.predict([vec])[0]
    cls = label_to_name[label]
    print name, 'is a document about', cls
 
    print '\n'
"""

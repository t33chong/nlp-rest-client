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
from multiprocessing import Pool
from boto import connect_s3
from tempfile import NamedTemporaryFile

topN = sys.argv[1]

num_topics = int(sys.argv[2])

wids = [str(int(line)) for line in open('topwams.txt').readlines()][:int(topN)]


def getData(wid):
    useCaching(perServiceCaching={'TopEntitiesService.get': {'dont_compute':True}, 'HeadsCountService.get': {'dont_compute':True}})
    return [(wid, [HeadsCountService().nestedGet(wid), TopEntitiesService().nestedGet(wid)])]

def vec2dense(vec, num_terms):
    '''Convert from sparse gensim format to dense list of numbers'''
    return list(gensim.matutils.corpus2dense([vec], num_terms=num_terms).T[0])

print "Loading entities and heads..."
entities = []
for result in Pool(processes=8).map(getData, wids):
    entities += result
entities = dict(entities)

widToEntityList = {}
for wid in entities:
    widToEntityList[wid] = []
    for entity in entities[wid][0]:
        widToEntityList[wid] += [entity] * int(entities[wid][0][entity])
    for entity in entities[wid][1]:
        widToEntityList[wid] += [entity[0]] * int(entity[1])

print len(widToEntityList)

print "Extracting to dictionary..."

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

modelname = 'lda-%swikis-%stopics.model' % (sys.argv[1], sys.argv[2])

key = connect_s3().get_bucket('nlp-data').get_key('models/'+modelname)
if os.path.exists(os.getcwd()+'/'+modelname):
    print "(loading from file)"
    lda_model = gensim.models.LdaModel.load(os.getcwd()+'/'+modelname)
else:
    print os.getcwd()+'/'+modelname, "does not exist"
    if key is not None:
        print "(loading from s3)"
        with open('/tmp/modelname', 'w') as fl:
            key.get_contents_to_file(fl)
        lda_model = gensim.models.LdaModel.load('/tmp/modelname')
    else:
        print "(building...)"
        lda_model = gensim.models.LdaModel(bow_docs.values(),
                                           num_topics=num_topics,
                                           id2word=dict([(x[1], x[0]) for x in dct.token2id.items()]))
        print "Done, saving model."
        lda_model.save(modelname)

print "Writing topics to files"
with open('%swiki-%stopics-sparse-topics.csv' % (sys.argv[1], sys.argv[2]), 'w') as sparse_csv:
    with open('%swiki-%stopics-dense-topics.csv' % (sys.argv[1], sys.argv[2]), 'w') as dense_csv:
        for name in widToEntityList:
            vec = bow_docs[name]
            sparse = lda_model[vec]
            dense = vec2dense(sparse, num_topics)
            lda_docs[name] = sparse
            sparse_csv.write(",".join([str(wid)]+['%d-%.8f' % x for x in sparse])+"\n")
            dense_csv.write(",".join([wid]+['%.8f' % x for x in list(dense)])+"\n")

print "Done"

if key is None:
    print "uploading model to s3"
    key = Key(connect_s3().get_bucket('nlp_data'))
    key.set_contents_from_file(modelname)

from pprint import pprint
from nlp_client.caching import useCaching
from nlp_client.services import TopEntitiesService
from wiki_recommender import as_euclidean

useCaching()

def cluster(wid):
    for wiki in as_euclidean(wid)[1]:
        print TopEntitiesService().nestedGet(wiki['id'])

if __name__ == '__main__':
    cluster('831')

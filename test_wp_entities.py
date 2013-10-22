from nlp_client.services import WpTopEntitiesService
from nlp_client.caching import useCaching
import sys

useCaching()

print WpTopEntitiesService().nestedGet(sys.argv[1])

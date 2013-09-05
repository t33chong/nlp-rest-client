import sys
from nlp_client import services

try:
    #services.useCaching()
    te = services.TopEntitiesService()
    print te.get(sys.argv[1])
except KeyboardInterrupt:
    sys.exit()

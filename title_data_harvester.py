import sys
import time
from nlp_client.services import RedirectsService, AllTitlesService
from nlp_client.caching import useCaching

start = time.time()
useCaching(writeOnly=True)
titles = AllTitlesService().nestedGet(sys.argv[1])
redirects = RedirectsService().nestedGet(sys.argv[1])
print "Finished %s in %d seconds (%d titles, %d redirects)" % (sys.argv[1], int(time.time() - start), len(titles), len(redirects))

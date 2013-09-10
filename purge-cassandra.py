'''
Allows different dimentionalities of cache purging from Cassandra.
'''
from nlp_client import services
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-s', '--service', dest='service', default=None,
                  help="The Service.method you want to purge")
parser.add_option('-d', '--doc_id', dest='doc_id', default=None,
                  help="The doc id you want to purge responses for")
parser.add_option('-w', '--wiki_id', dest='wiki_id', default=None,
                  help="The wiki id you want to purge responses for")

(options, args) = parser.parse_args()


if not options.service and not options.doc_id and not options.wiki_id:
    raise ValueError("Need to specify a type of purge")

services.useCaching()

if options.service:
    services.purgeCacheForService(options.service)
elif options.doc_id:
    services.purgeCacheForDoc(options.doc_id)
elif options.wiki_id:
    services.purgeCacheForWiki(options.wiki_id)

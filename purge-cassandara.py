from nlp_client import services
from OptParse import OptionParser

parser = OptionParser()
parser.add_option('-s', '--service', dest='service',
                  help="The Service.method you want to purge")
parser.add_option('-d', '--doc_id', dest='doc_id',
                  help="The doc id you want to purge responses for")
parser.add_option('-w', '--wiki_id' dest='wiki_id',
                  help="The wiki id you want to purge responses for")

(options, args) = parser.parse_args()

optiondict = dict(options)
if len(optiondict.keys()) == 0:
    raise ValueError("Need to specify a type of purge")

services.useCaching()

service, doc_id, wiki_id = optiondict.get('service', None), optiondict.get('doc_id', None), optiondict.get('wiki_id', None)

if service:
    print services.purgeCacheForService(service)
elif doc_id:
    print services.purgeCacheForDoc(doc_id)
elif wiki_id:
    print service.purgeCacheForWiki(wiki_id)

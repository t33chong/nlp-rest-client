from nlp_client.services import DocumentSentimentService
import sys

print DocumentSentimentService().get(sys.argv[1])

from nlp_client.wiki_parses import *
import sys

print main_page_nps(sys.argv[1])
print 'sitename'
print phrases_for_wiki_field(sys.argv[1], 'sitename_txt')
print 'description'
print phrases_for_wiki_field(sys.argv[1], 'description_txt')
print 'headline'
print phrases_for_wiki_field(sys.argv[1], 'headline_txt')

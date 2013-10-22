import sys
from nlp_client.title_confirmation import check_wp

print check_wp(" ".join(sys.argv[1:]))

from nlp_client import title_confirmation as tc
import sys

print 'hi'
connection = tc.get_sqlite_connection()

if len(sys.argv) > 1:
    print "Testing connection:"
    print "gambia", tc.check_wp("gambia")
    print "garbage", tc.check_wp("asdkfjakgjagkjag")

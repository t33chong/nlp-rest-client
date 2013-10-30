from nlp_client import services, caching
import boto
import sys
import re

BUCKET = boto.connect_s3().get_bucket('nlp-data')

caching.use_caching()

def processFile(filename):
    try:
        match = re.search('([0-9]+)/([0-9]+)', filename)
        doc_id = '%s_%s' % (match.group(1), match.group(2))
        try:
            DocumentEntitySentimentService().get(doc_id)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            print 'Could not call %s on %s!' % (service, doc_id)
    except AttributeError:
        print 'Unexpected format: %s:' % (filename)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    except KeyboardInterrupt:
        sys.exit()


def call_services(keyname):
    global BUCKET

    key = BUCKET.get_key(keyname)
    if key is None:
        return

    SIG = "%s_%s_%s" % (boto.utils.get_instance_metadata()['local-hostname'], str(time.time()), str(int(random()*100)))
    eventfile = 'data_processing/'+SIG
    try:
        key.copy('nlp-data', eventfile)
        key.delete()
    except S3ResponseError:
        print 'EVENT FILE %s NOT FOUND!' % eventfile
        return
    except KeyboardInterrupt:
        sys.exit()

    print 'STARTING EVENT FILE %s' % eventfile
    k = Key(BUCKET)
    k.key = eventfile

    map(processFile, k.get_contents_as_string().split(u'\n'))
            
    print 'EVENT FILE %s COMPLETE' % eventfile
    k.delete()

call_services(sys.argv[1])

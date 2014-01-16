"""
This script polls S3 to find new text batches to parse.
"""
from autoscale_parser import EC2RegionConnection
from boto import connect_s3, connect_ec2
from boto.s3.key import Key
#from boto.ec2 import connect_to_region
from boto.exception import S3ResponseError
from boto.utils import get_instance_metadata
from time import time, sleep
from socket import gethostname
from subprocess import Popen, call
import tarfile
import os
import shutil
import sys

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

SIG = str(os.getpid()) + '_' + str(int(time()))
TEXT_DIR = '/tmp/text/'
XML_DIR = '/tmp/xml/'
PACKAGE_DIR = "/tmp/event_packages/"
BUCKET_NAME = 'nlp-data'
REGION = 'us-west-2'
DESIRED_CAPACITY = 4

s3_conn = connect_s3()
bucket = s3_conn.get_bucket(BUCKET_NAME)
hostname = gethostname()
ec2_conn = EC2RegionConnection(REGION)
stalling_increments = 0

def add_files():
    global hostname, bucket, PACKAGE_DIR, SIG, inqueue
    print "[%s] Adding to text queue" % hostname

    keys = filter(lambda x:x.key.endswith('.tgz'), bucket.list('text_events/'))

    # iterating over keys in case we try to grab a key that another instance scoops
    for key in keys:
        old_key_name = key.key
        print "[%s] found key %s" % (hostname, old_key_name)
        # found a tar file, now try to capture it via move
        try:
            new_key_name = '/parser_processing/'+SIG+'.tgz'
            key.copy(bucket, new_key_name)
            key.delete()
            
        except S3ResponseError:
            # we probably hit our race condition -- not to worry!
            # we'll just take the next key.
            continue

        # now that it's been moved, pull it down
        newkey = Key(bucket)
        newkey.key = new_key_name
        newfname = PACKAGE_DIR+SIG+'.tgz'
        newkey.get_contents_to_filename(newfname)
        
        # untar that sucker
        print "[%s] Unpacking %s" % (hostname, newfname)
        tar = tarfile.open(newfname)
        tar.extractall(TEXT_DIR)
        tar.close()
        os.remove(newfname)
        inqueue = len(os.listdir(TEXT_DIR))

        # delete remnant data with extreme prejudice
        newkey.delete()

        # at this point we want to get the list of keys all over again
        return True
    return False

while True:
    for directory in [TEXT_DIR, XML_DIR, PACKAGE_DIR]:
        if not os.path.exists(directory):
            os.mkdir(directory)

    inqueue = len(os.listdir(TEXT_DIR))
    
    if inqueue < 10:
        added = add_files()
        # shut this instance down if we have an empty queue and we're above desired capacity
        if not added and len(os.listdir(XML_DIR)) == 0 and len(os.listdir(TEXT_DIR)):
            instances = ec2_conn.get_tagged_instances()
            if DESIRED_CAPACITY < len(instances):
                print "[%s] Scaling down, shutting down." % hostname
                current_id = get_instance_metadata()['instance-id']
                if len(filter(lambda x: x.instance_id == current_id, instances)) == 1:
                    ec2_conn.terminate([current_id])
                    sys.exit()

    print "[%s] %d text files in queue..." % (hostname, inqueue)

    data_events = []
    xmlfiles = os.listdir(XML_DIR)

    if len(xmlfiles) == 0:
        stalling_increments += 1
    else:
        stalling_increments = 0

    if stalling_increments == 5:
        stalling_increments = 0
        print "Restarting daemon and adding more files since it's being a slow douche."
        call("sv stop parser_daemon", shell=True)
        call("killall java", shell=True)
        call("sv start parser_daemon", shell=True)
        print "Done with that. Now get to work!"

    for xmlfile in xmlfiles:
        key = Key(bucket)
        new_key = '/xml/%s/%s.xml' % tuple(xmlfile.replace('.xml', '').split('_'))
        key.key = new_key
        data_events += [new_key]
        xmlfilename = XML_DIR+xmlfile
        key.set_contents_from_filename(xmlfilename)
        os.remove(xmlfilename)

    print "[%s] Uploaded %d files (rate of %.2f docs/sec)" % (hostname, len(xmlfiles), float(len(xmlfiles))/30.0)
        
    # write events to a new file
    event_key = Key(bucket)
    event_key.key = '/data_events/'+SIG
    event_key.set_contents_from_string("\n".join(data_events))

    sleep(30) # don't want to bug the crap outta amazon

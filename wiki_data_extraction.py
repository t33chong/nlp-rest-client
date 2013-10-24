import random
import sys
import os
from time import sleep
from subprocess import Popen, STDOUT
from boto import connect_s3
from boto.s3.prefix import Prefix

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

warmOnly = False
firstTime = True
while True:
    print "Getting wids"
    if not firstTime:
        wids = [prefix.name.split('/')[-2] for prefix in connect_s3().get_bucket('nlp-data').list(prefix='xml/', delimiter='/') if isinstance(prefix, Prefix)]
        random.shuffle(wids)
    else:
        wids = [str(int(id)) for id in open('topwams.txt')]
        random.shuffle(wids)
        wids = wids[:1000]
    print "Working on %d wids" % len(wids)

    processes = []
    while len(wids) > 0:
        while len(processes) < 8 and len(wids) > 0:
            popen_params = ['/usr/bin/python', 'wiki_data_extraction_child.py', wids.pop()]
            if warmOnly:
                popen_params += ['1']
            processes += [Popen(popen_params)]

        processes = filter(lambda x: x.poll() is None, processes)
        sleep(0.25)

    warmOnly = False
    firstTime = True
    print "Finished"

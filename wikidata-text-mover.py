import os
import shutil
import sys

if not os.path.exists('/data/wiki_xml/'):
    os.mkdir('/data/wiki_xml/')

if len(sys) > 1:
    print "Starting up and moving text to parser tmp"
    shtuil.copytree('/data/wiki_text/', '/tmp/text/')
else:
    print "Skipping moving text over"

while True:
    for fl in os.listdir('/tmp/xml/'):
        wid, field = fl.split('_')
        if not os.path.exists('/data/wiki_xml/%s' % wid):
            os.mkdir('/data/wiki_xml/%s' % wid)
        shutil.move('/tmp/xml/'+fl, '/tmp/wiki_xml/%s/%s' % (wid, field)))

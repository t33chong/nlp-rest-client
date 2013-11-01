import os
import shutil
import sys

if not os.path.exists('/data/wiki_xml/'):
    os.mkdir('/data/wiki_xml/')

if len(sys.argv) > 1:
    print "Starting up and moving text to parser tmp"
    [shutil.copy('/data/wiki_text/%s' % fl, '/tmp/text/%s' % fl) for fl in os.listdir('/data/wiki_text/')]
else:
    print "Skipping moving text over"

while True:
    for fl in os.listdir('/tmp/xml/'):
        spl = fl.split('_')
        wid = spl[0]
        field = "_".join(spl[1:])
        wiki_xml_dir = '/data/wiki_xml/%s' % wid
        if not os.path.exists(wiki_xml_dir):
            os.mkdir(wiki_xml_dir)
        shutil.move('/tmp/xml/'+fl, '%s/%s' % (wiki_xml_dir, field))

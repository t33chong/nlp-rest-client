"""
Tar the contents of a directory and delete the original directory.
If LOCAL is False (0), upload tarball to an S3 bucket, and delete the original.
"""

import os
import sys
import shutil
import tarfile

DIRECTORY = sys.argv[1]
LOCAL = sys.argv[2]

# archive
tarname = DIRECTORY + '.tgz'
tar = tarfile.open(tarname, 'w:gz')
tar.add(DIRECTORY, '.')
tar.close()

# remove original directory
shutil.rmtree(DIRECTORY)

if not bool(int(LOCAL)):
    # upload to AWS
    bucket = S3Connection().get_bucket('nlp-data')
    k = Key(bucket)
    k.key = 'text_events/%s' % os.path.basename(tarname)
    k.set_contents_from_filename(tarname)

    # remove tar file
    os.remove(tarname)

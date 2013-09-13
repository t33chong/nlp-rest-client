from flask import Flask, request
from flask.ext import restful
from boto import connect_s3
from boto.s3.key import Key

import os
import time
import json



class EventResource(restful.Resource):

    ''' Parent class for services '''

    def _writeToQueue(self, queryList):
        ''' Writes a set of queries to a flat file 
        :param queryList: a list of query strings
        '''

        if len(queryList) == 0:
            return # no need to make an empty file

        fname = '/data/events/%s_%s' % (os.getpid(), int(time.time()))
        with open(fname, 'w') as f:
            f.write("\n".join(queryList))


    def _deleteFromS3(self, path):
        ''' Deletes a path from the nlp-data bucket 
        :param path: the string value of the path or file we want to delete
        '''

        conn = boto.connect_s3()
        bucket = conn.get_bucket('nlp-data')
        try:
            Key(path).delete()
        except:
            pass
        

class DocEventService(EventResource):

    ''' Capable of handling events related to single documents '''

    def post(self):
        ''' Given a param "ids" which is a list of ids, 
        write a single-document query for each id to the queue 
        '''

        self._writeToQueue(['id:%s'%id for id in request.json.get('ids', [])])
        return {'status': 200}


    def delete(self, doc_id):
        ''' Delete document's xml
        :param doc_id: the document ID
        '''

        self._deleteFromS3("xml/%s/%s.xml" % tuple(doc_id.split('_')))
        return {'status': 200}


class WikiEventService(EventResource):

    ''' Capable of handling events related to an entire wiki '''

    def post(self, wiki_id):
        ''' Writes a query for all content pages for that wiki to queue file 
        :param wiki_id: the int id of the wiki
        '''

        self._writeToQueue(['wid:%d AND iscontent:true' % wiki_id])
        return {'status': 200}
        

    def delete(self, wiki_id):
        ''' Directly deletes wiki ID folder from S3 
        :param wiki_id: the int id of the wiki
        '''

        if not wiki_id:
            # this is just being super, super safe, because no wiki ID would delete all xml
            raise ValueError("A wiki ID is required")
        self._deleteFromS3('xml/%d' % wiki_id)
        return {'status': 200}


app = Flask(__name__)
api = restful.Api(app)

api.add_resource(DocEventService,          '/docs/')
api.add_resource(WikiEventService,         '/wiki/<int:wiki_id>/')

if __name__ == '__main__':
    config = json.loads("\n".join(open('aws.json').readlines()))
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = config.get('access_key', None), config.get('secret_key', None)
    app.run(debug=True, host='0.0.0.0')

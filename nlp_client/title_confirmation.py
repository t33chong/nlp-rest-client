from flask import Flask, request
try:
    from wikicities.DB import LoadBalancer
except:
    pass #screw it
from boto import connect_s3
from optparse import OptionParser
from gzip import GzipFile, open as gzopen
from StringIO import StringIO
from urllib import quote_plus
from nltk.corpus import stopwords
import os
import sys
import zlib
import phpserialize
import re
import json
import time
import sqlite3 as lite

""" Memoization variables """
TITLES, REDIRECTS, CURRENT_WIKI_ID, USE_S3, WP_SEEN, ALL_WP, SQLITE_CONNECTION = [], {}, None, True, [], {}, None

yml = '/usr/wikia/conf/current/DB.yml'
app = Flask(__name__)

def get_config():
    return yml

def get_local_db_from_options(options, global_db):
    """ Allows us to load in the local DB name from one or more options
    :param options: the 0th result of OptionParser.parse_args()
    """
    if options.id:
        where = "city_id = %s" % options.id
    elif options.wikihost:
        where = 'city_url = "%s"' % options.wikihost
    elif options.db:
        where = 'city_dbname = "%s"' % options.db
    else:
        raise ValueError("Need a db, id, or host.")

    cursor = global_db.cursor()
    sql = "SELECT city_id, city_dbname FROM city_list WHERE %s" % where
    results = cursor.execute(sql)
    result = cursor.fetchone()
    if not result:
        raise ValueError("No wiki found")

    return result

def get_global_db(master=False):
    lb = LoadBalancer(get_config())
    return lb.get_db_by_name('wikicities', master=master)

def get_local_db_from_wiki_id(global_db, wiki_id, master=False):
    global CURRENT_WIKI_ID
    cursor = get_global_db().cursor()
    sql = "SELECT city_id, city_dbname FROM city_list WHERE city_id = %s" % str(wiki_id)
    results = cursor.execute(sql)
    result = cursor.fetchone()
    if not result:
        raise ValueError("No wiki found")

    CURRENT_WIKI_ID = result[0]
    return LoadBalancer(get_config()).get_db_by_name(result[1], master=master) 

def get_namespaces(global_db, wiki_id):
    """ Accesses the default content namespaces for the wiki
    :param global_db: the global database object
    """
    cursor = global_db.cursor()
    results = cursor.execute("SELECT cv_value FROM city_variables WHERE cv_city_id = %s AND cv_variable_id = 359" % str(wiki_id))
    result = cursor.fetchone()
    return phpserialize.loads(result[0]).values() if result else [0, 14]


def define_options():
    """ Subroutine for option-handling """
    parser = OptionParser()
    parser.add_option("-i", "--id", dest="id", action="store", default=None,
                      help="Specifies the wiki ID")
    parser.add_option("-w", "--wikihost", dest="wikihost", action="store", default=None,
                      help="Specifies the wiki host")
    parser.add_option("-d", "--db", dest="db", action="store", default=None,
                      help="Specifies the databse to use (preferred)")
    parser.add_option("-c", "--dbconfig", dest="dbconfig", action="store", default=None,
                      help="DB config file (required)")
    (options, args) = parser.parse_args()
    return options


def preprocess(title):
    """ Mutate each title to the appropriate pre-processed value
    :param row: cursor title
    """
    stops = stopwords.words('english')
    return ' '.join(filter(lambda x: x not in stops, re.sub(' \(\w+\)', '', title.lower().replace('_', ' ')).split(' ')))[:500] #500 chars should be plenty, todo fix unicode shit

def check_wp(title):
    """ Checks if a "title" is a title in wikipedia first using memoization cache, then check_wp_s3
    :param title: string
    """
    global WP_SEEN
    ppt = preprocess(title)
    bool = ppt in WP_SEEN or check_wp_sqlite(ppt)
    return bool 


def check_wp_sqlite(title):
    global WP_SEEN
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM `titles` where `title` = \"%s\"" % (title.replace('"', '""')))
    if cursor.fetchone() is not None:
        WP_SEEN += [title]
        return True
    return False


def get_titles_for_wiki_id(wiki_id):
    global TITLES, CURRENT_WIKI_ID, USE_S3
    if wiki_id == CURRENT_WIKI_ID and len(TITLES) > 0:
        return TITLES

    if USE_S3:
        bucket = connect_s3().get_bucket('nlp-data')
        key = bucket.get_key('article_titles/%s.gz' % str(wiki_id))
        io = StringIO()
        key.get_file(io)
        io.seek(0)
        stringdata = GzipFile(fileobj=io, mode='r').read().decode('ISO-8859-2').encode('utf-8')
        TITLES = json.loads(stringdata)[wiki_id]
    else:
        local_db = get_local_db_from_wiki_id(get_global_db(), wiki_id)
        CURRENT_WIKI_ID = wiki_id
        cursor = local_db.cursor()
        cursor.execute("SELECT page_title FROM page WHERE page_namespace IN (%s)" % ", ".join(map(str, get_namespaces(get_global_db(), wiki_id))))
        TITLES = set(map(lambda x: preprocess(x[0]), cursor))

    CURRENT_WIKI_ID = wiki_id
    return TITLES

def get_redirects_for_wiki_id(wiki_id):
    global REDIRECTS, CURRENT_WIKI_ID, USE_S3
    if wiki_id == CURRENT_WIKI_ID and len(REDIRECTS) > 0:
        return REDIRECTS

    if USE_S3:
        bucket = connect_s3().get_bucket('nlp-data')
        key = bucket.get_key('article_redirects/%s.gz' % str(wiki_id))
        io = StringIO()
        key.get_file(io)
        io.seek(0)
        stringdata = GzipFile(fileobj=io, mode='r').read()
        REDIRECTS = json.loads(stringdata)[wiki_id]
    else:
        local_db = get_local_db_from_wiki_id(get_global_db(), wiki_id)
        cursor = local_db.cursor()
        cursor.execute("SELECT page_title, rd_title FROM redirect INNER JOIN page ON page_id = rd_from")
        REDIRECTS = dict([map(preprocess, row) for row in cursor])

    CURRENT_WIKI_ID = wiki_id
    return REDIRECTS

def get_sqlite_connection():
    global SQLITE_CONNECTION
    if SQLITE_CONNECTION is None:
        SQLITE_CONNECTION = bootstrap_sqlite_connection()
    return SQLITE_CONNECTION

def bootstrap_sqlite_connection():
    if not os.path.exists(os.getcwd()+'/wp_titles.db'):
        print 'downloading'
        """
        THIS ISN'T WORKING RIGHT NOW! i think there's a problem with how it's stored in S3. For now, AMIs and shit.
        """
        key = connect_s3().get_bucket('nlp-data').get_key('wp_titles.db')
        if key is not None:
            key.get_contents_to_filename(os.getcwd()+'wp_titles.db')
    conn = lite.connect('wp_titles.db')
    conn.text_factory = str
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='titles'")
    if cursor.fetchone() is None:
        pass
        #create_wp_table(conn)
    return conn

def create_wp_table(conn):
    print 'creating'
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS `titles`
                    (title TEXT UNIQUE);''')

    print "Extracting/Inserting..."
    counter = 0
    for line in list(set(map(lambda x: preprocess(x.strip()), gzopen('/'.join(os.path.realpath(__file__).split('/')[:-1])+'/enwiki-20131001-all-titles-in-ns0.gz')))):
        cur.execute("INSERT INTO `titles` (`title`) VALUES (?)", (line,))
        counter += 1
        if counter % 500 == 0:
            print counter

    print "Committing..."
    conn.commit()

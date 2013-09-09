from flask import Flask, request
from wikicities.DB import LoadBalancer
from optparse import OptionParser
import phpserialize
import re
import json
import time

""" Ultimately populated with processed titles for the wiki we're serving """
TITLES = []

app = Flask(__name__)

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


def get_namespaces(global_db, wiki_id):
    """ Accesses the default content namespaces for the wiki
    :param global_db: the global database object
    """
    cursor = global_db.cursor()
    results = cursor.execute("SELECT cv_value FROM city_variables WHERE cv_city_id = %d AND cv_variable_id = 359" % wiki_id)
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
    return re.sub(' \(\w+\)', '', title.lower().replace('_', ' ')) #todo fix unicode shit


@app.route("/", methods=['POST'])
def is_title_legit():
    """
    Checks if POSTed titles are legit, and gives redirects
    TODO: use bloom filter!?
    """
    global TITLES, REDIRECTS
    titles = request.json.get('titles', [])
    print titles, map(preprocess, titles)
    resp = {}
    checked_titles = map(lambda x: (x, x in TITLES), map(preprocess, titles))
    resp['titles'] = dict(checked_titles)
    redirectkeys = REDIRECTS.keys()
    resp['redirects'] = dict(map(lambda x: (x[0], REDIRECTS[x[0]]), filter(lambda x: x[0] in redirectkeys and x[1], checked_titles)))
    return json.dumps(resp)


def main(app):
    global TITLES, REDIRECTS
    start = time.time()
    print "Starting title server..."

    options = define_options()
    lb = LoadBalancer(options.dbconfig)
    global_db = lb.get_db_by_name('wikicities')
    (wiki_id, dbname) = get_local_db_from_options(options, global_db)
    local_db = lb.get_db_by_name(dbname)

    # store flat, unique dictionary
    cursor = local_db.cursor()
    cursor.execute("SELECT page_title FROM page WHERE page_namespace IN (%s)" % ", ".join(map(str, get_namespaces(global_db, wiki_id))))
    TITLES = set(map(lambda x: preprocess(x[0]), cursor))

    # relate preprocessed redirect title to canonical title
    cursor = local_db.cursor()
    cursor.execute("SELECT page_title, rd_title FROM redirect INNER JOIN page ON page_id = rd_from")
    REDIRECTS = dict([map(preprocess, row) for row in cursor])

    print "Title server ready in %s seconds" % str(time.time() - start)
    app.run(debug=True, host='0.0.0.0')


if __name__ == '__main__':
    main(app)

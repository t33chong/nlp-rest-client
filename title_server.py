from flask import Flask
from wikicities.DB import LoadBalancer
from optparse import OptionParser
import phpserialize


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
    return phpserialize.loads(result[0]).values() if len(result) else [0, 14]


def preprocess(row):
    """
    This is responsible for mutating each title to the appropriate pre-processed value
    :param row: cursor row
    """
    title = row[0]
    return unicode(title, 'utf-8')


def define_options():
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


def main():
    options = define_options()
    lb = LoadBalancer(options.dbconfig)
    global_db = lb.get_db_by_name('wikicities')
    (wiki_id, dbname) = get_local_db_from_options(options, global_db)
    local_db = lb.get_db_by_name(dbname)

    cursor = local_db.cursor()
    cursor.execute("SELECT page_title FROM page WHERE page_namespace IN (%s)" % ", ".join(map(str, get_namespaces(global_db, wiki_id))))

    titles = map(preprocess, cursor)

    print titles


if __name__ == '__main__':
    main()

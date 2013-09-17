import json
import sys
from cql import connection

config = json.loads("".join(open('config.json').readlines()))['cassandra']
conn = connection.connect(config['host'], int(config['port']), 'nlp')
cursor = conn.cursor()

"""
Add a function here to migrate
"""
roll_forward = [ \
    lambda cursor: map(lambda query: cursor.execute(query), \
          ["""
           CREATE TABLE service_responses (
             signature varchar PRIMARY KEY,
             doc_id varchar,
             service varchar,
             wiki_id int,
             response text,
             last_updated int
           );""",
           "CREATE INDEX ON service_responses (wiki_id);",
           "CREATE INDEX ON service_responses (service);",
           "CREATE INDEX ON service_responses (doc_id);",
           ]
                       )
]

"""
In the same order as roll_forward -- not used for now
"""
roll_backward =  [ \
    lambda cursor: cursor.execute("DROP TABLE service_responses")
]

if len(sys.argv) > 1:
    map(lambda fn: fn(cursor), roll_backward)
else:
    map(lambda fn: fn(cursor), roll_forward)

print "Migration complete"

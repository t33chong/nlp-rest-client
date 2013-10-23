import phpserialize
import sys

CV_ID = 1344

fname = sys.argv[1]

def csv2sql(line):
    global CV_ID
    line = line.split(',')
    vals = [x.split('-') for x in line[1:]]
    return "(%s, %s, \"%s\")" % (line[0], CV_ID, phpserialize.dumps([x[0] for x in sorted(vals, reverse=True, key=lambda x: float(x[1]))]).replace('"', '""'))

with open(fname) as csv:
    with open(fname+'.sql', 'w') as sqlfile:
        sqlfile.write("INSERT INTO city_variables (`cv_city_id`, `cv_variable_id`, `cv_value`) VALUES %s;"
                          % (",\n".join(map(csv2sql, csv))))
    
        

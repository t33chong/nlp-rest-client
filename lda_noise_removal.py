import sys
from collections import defaultdict
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-i', '--input', dest='input_file', action='store', help='Location of CSV file from which to remove high-frequency features')
parser.add_option('-o', '--output', dest='output_file', action='store', default=False, help='Location of CSV file to write to')
parser.add_option('-m', '--max-freq', dest='maxfreq', action='store', default=500, help='Integer frequency above which a feature will be omitted')
options, args = parser.parse_args()

if not options.output_file:
    options.output_file = 'clean-' + options.input_file

tally = defaultdict(int)

with open(options.input_file) as input_file:
    for line in input_file:
        data = line.strip().split(',')
        if len(data) > 1:
            for pair in data[1:]:
                feature, frequency = pair.split('-')
                tally[int(feature)] += 1

with open(options.input_file) as input_file:
    with open(options.output_file, 'w') as output_file:
        for line in input_file:
            data = line.strip().split(',')
            output = data[0]
            if len(data) > 1:
                pairs = [pair for pair in data[1:] if tally[int(pair.split('-')[0])] < options.maxfreq]
                if pairs: output += ',' + ','.join(pairs)
            output_file.write(output + '\n')

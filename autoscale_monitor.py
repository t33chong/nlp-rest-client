from boto import connect_s3
from boto.ec2.autoscale import connect_to_region
from optparse import OptionParser
from time import sleep
from datetime import datetime

"""
Monitors the workload in specific intervals and scales up or down
We need this script for two reasons:
1) You can't create metric alarms based off of stuff in S3
2) You can't create metric alarms for EC2 instances hosted outside of us-east-1
"""

GROUP_NAME = 'parser'
THRESHOLD = 10

QUEUES = {
    'parser': 'text_events',
    'data_extraction': 'data_events'
}

parser = OptionParser()
parser.add_option('-t', '--threshold', type='int', dest='threshold', default=THRESHOLD,
                  help='Acceptable amount of events per process we will tolerate being backed up on')
parser.add_option('-g', '--group', dest='group', default=GROUP_NAME,
                  help='The autoscale group name to operate over')

(options, args) = parser.parse_args()

conn = connect_s3()
bucket = conn.get_bucket('nlp-data')
autoscale = connect_to_region('us-west-2')

lastInQueue = None
intervals = []
while True:
    group = autoscale.get_all_groups(names=[options.group])[0]
    inqueue = len([k for k in bucket.list(QUEUES[options.group])]) - 1 #because it lists itself, #lame

    if lastInQueue is not None and lastInQueue != inqueue:
        delta = (lastInQueue - inqueue) 
        intervals += [(mins, delta * 250)]
        avg = reduce(lambda x, y: x + y, map(lambda x: float(x[1])/float(x[0]*60), intervals))/len(intervals);
        rate = ", %.3f docs/sec; %d in the last %d minute(s)" % (avg, delta * 250, mins)
    else:
        rate = ""


    numinstances = len([i for i in group.instances]) # stupid resultset object

    events_per_instance = (float(inqueue) / float(numinstances))
    above_threshold =  events_per_instance > options.threshold

    if group.max_size > numinstances and above_threshold:
        currinstances = numinstances
        while (float(inqueue) / float(currinstances)) > options.threshold and currinstances < group.max_size:
            autoscale.execute_policy('scale_up', options.group)
            currinstances += 1
        print "[%s %s] Scaled up to %d (%d in queue%s)" % (group.name, datetime.today().isoformat(' '), currinstances, inqueue, rate) 
    else:
        print "[%s %s] Just chillin' (%d in queue, %d instances%s)" % (group.name, datetime.today().isoformat(' '), inqueue, numinstances, rate)

    if inqueue == lastInQueue:
        mins += 1
    else:
        mins = 1

    lastInQueue = inqueue
    sleep(60)

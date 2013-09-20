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

GROUP_NAME = 'parser_poller'
THRESHOLD = 10

QUEUES = {
    'parser_poller': 'text_events',
    'data_poller': 'data_events'
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

while True:
    group = autoscale.get_all_groups(names=[options.group])[0]
    inqueue = len([k for k in bucket.list(QUEUES[options.group])]) - 1 #because it lists itself, #lame

    numinstances = len([i for i in group.instances]) # stupid resultset object

    events_per_instance = (float(inqueue) / float(numinstances))
    above_threshold =  events_per_instance > options.threshold

    if group.max_size > numinstances and above_threshold:
        currinstances = numinstances
        while (float(inqueue) / float(currinstances)) > options.threshold and currinstances < group.max_size:
            autoscale.execute_policy('scale_up', options.group)
            currinstances += 1
        print "[%s %s] Scaled up to %d (%d in queue)" % (group.name, datetime.today().isoformat(' '), currinstances, inqueue) 
    else:
        print "[%s %s] Just chillin' (%d in queue, %d instances)" % (group.name, datetime.today().isoformat(' '), inqueue, numinstances)

    sleep(60)

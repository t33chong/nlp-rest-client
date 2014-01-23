from __future__ import division
from autoscale_parser import opt
from optparse import OptionParser

"""
Monitors the workload in specific intervals and scales up or down
We need this script for two reasons:
1) You can't create metric alarms based off of stuff in S3
2) You can't create metric alarms for EC2 instances hosted outside of us-east-1
"""

COUNT = 0
REGION = 'us-west-2'
PRICE = '0.300'
AMI = 'ami-403e5e70'
KEY = 'data-extraction'
SECURITY_GROUPS = 'sshable'
INSTANCE_TYPE = 'm2.4xlarge'
TAG = 'parser'
THRESHOLD = 10
MAX_SIZE = 4

QUEUES = {
    'parser': 'text_events',
    'data_extraction': 'data_events'
}

op = OptionParser()
op.add_option('-r', '--region', dest='region', default=REGION,
              help='The EC2 region to connect to')
op.add_option('-p', '--price', dest='price', default=PRICE,
              help='The maximum bid price')
op.add_option('-a', '--ami', dest='ami', default=AMI,
              help='The AMI to use')
op.add_option('-c', '--count', dest='count', default=COUNT, type='int',
              help='The number of instances desired')
op.add_option('-k', '--key', dest='key', default=KEY,
              help='The name of the key pair')
op.add_option('-s', '--security-groups', dest='sec', default=SECURITY_GROUPS,
              help='The security groups with which to associate instances')
op.add_option('-i', '--instance-type', dest='type', default=INSTANCE_TYPE,
              help='The type of instance to run')
op.add_option('-t', '--tag', dest='tag', default=TAG,
              help='The tag name to operate over')
op.add_option('-e', '--threshold', dest='threshold', default=THRESHOLD, type='int',
              help='Acceptable number of events per process we will tolerate as' +
                   'backlog')
op.add_option('-m', '--max-size', dest='max_size', default=MAX_SIZE, type='int',
              help='The maximum allowable number of simultaneous instances')
(options, args) = op.parse_args()

opt.update(vars(options))

from autoscale_parser import EC2RegionConnection
from boto import connect_s3
from boto.ec2.autoscale import connect_to_region
from datetime import datetime
from math import ceil
from time import sleep

conn = connect_s3()
bucket = conn.get_bucket('nlp-data')
autoscale = connect_to_region('us-west-2')
ec2_conn = EC2RegionConnection(region=options.region)

lastInQueue = None
intervals = []
while True:
    inqueue = len([k for k in bucket.list(QUEUES[options.tag])]) - 1 #because it lists itself, #lame
    instances = ec2_conn.get_tagged_instances(options.tag)
    numinstances = len(instances)

    if not inqueue:
        print "[%s %s] Just chillin' (%d in queue, %d instances)" % (options.tag, datetime.today().isoformat(' '), inqueue, numinstances)
        sleep(60)
        continue

    if not numinstances:
        optimal = int(ceil(inqueue / options.threshold))
        instances_to_add = optimal if optimal <= options.max_size else options.max_size
        ec2_conn.add_instances(instances_to_add)
        instances = ec2_conn.get_tagged_instances(options.tag)
        numinstances = len(instances)
        print "[%s %s] Scaled up to %d (%d in queue)" % (options.tag, datetime.today().isoformat(' '), numinstances, inqueue)
        continue

    if lastInQueue is not None and lastInQueue != inqueue:
        delta = (lastInQueue - inqueue)
        intervals += [(mins, delta * 250)]
        avg = reduce(lambda x, y: x + y, map(lambda x: x[1]/(x[0]*60), intervals))/len(intervals);
        rate = ", %.3f docs/sec; %d in the last %d minute(s)" % (avg, delta * 250, mins)
    else:
        rate = ""

    events_per_instance = inqueue / numinstances
    above_threshold =  events_per_instance > options.threshold

    if (options.max_size > numinstances and above_threshold):
        ratio = inqueue / numinstances
        while (ratio > options.threshold and numinstances < options.max_size):
            optimal = int(ceil(inqueue / options.threshold)) - numinstances
            allowed = options.max_size - numinstances
            instances_to_add = optimal if optimal <= allowed else allowed
            ec2_conn.add_instances(instances_to_add)
            instances = ec2_conn.get_tagged_instances(options.tag)
            numinstances = len(instances)
            ratio = inqueue / numinstances
        print "[%s %s] Scaled up to %d (%d in queue%s)" % (options.tag, datetime.today().isoformat(' '), numinstances, inqueue, rate)
    else:
        print "[%s %s] Just chillin' (%d in queue, %d instances%s)" % (options.tag, datetime.today().isoformat(' '), inqueue, numinstances, rate)

    if inqueue == lastInQueue:
        mins += 1
    else:
        mins = 1

    lastInQueue = inqueue
    sleep(60)

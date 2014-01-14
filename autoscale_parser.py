from boto.ec2 import connect_to_region
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from time import sleep

COUNT = 10
TAG = 'parser'
PRICE = '0.300'
AMI = 'ami-f48c14c4'
KEY = 'data-extraction'
SECURITY_GROUPS = 'sshable'
INSTANCE_TYPE = 'm2.4xlarge'

op = OptionParser()
op.add_option('-p', '--price', dest='price', default=PRICE,
              help='The maximum bid price')
op.add_option('-a', '--ami', dest='ami', default=AMI,
              help='The AMI to use')
op.add_option('-c', '--count', dest='count', default=COUNT, type='int',
              help='The number of instances to be requested')
op.add_option('-k', '--key', dest='key', default=KEY,
              help='The name of the key pair')
op.add_option('-s', '--security-groups', dest='sec', default=SECURITY_GROUPS,
              help='The security groups with which to associate instances')
op.add_option('-i', '--instance-type', dest='type', default=INSTANCE_TYPE,
              help='The type of instance to run')
op.add_option('-t', '--tag', dest='tag', default=TAG,
              help='The tag name to operate over')
(options, args) = op.parse_args()

tags = {'Name': options.tag}
filters = {'tag:Name': options.tag}

conn = connect_to_region('us-west-2')

reservation = conn.request_spot_instances(price=options.price,
                                          image_id=options.ami,
                                          count=options.count,
                                          key_name=options.key,
                                          security_groups=options.sec.split(','),
                                          instance_type=options.type)

print reservation

request_ids = [request.id for request in reservation]
print request_ids

while True:
    sleep(1)
    requests = conn.get_all_spot_instance_requests(request_ids=request_ids)
    instance_ids = []
    for request in requests:
        instance_id = request.instance_id
        print 'instance_id is %s' % instance_id
        if instance_id is None:
            break
        print 'appending %s' % instance_id
        instance_ids.append(instance_id)
    if len(instance_ids) < len(reservation):
        print 'not enough instance_ids'
        continue
    break

print 'instance_id collection complete'
print instance_ids

print conn.create_tags(instance_ids, tags)

instances = [instance.id for reservation in conn.get_all_instances(filters=filters) for instance in reservation.instances]
print instances

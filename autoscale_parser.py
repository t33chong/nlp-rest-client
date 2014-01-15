from boto.ec2 import connect_to_region
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from time import sleep

COUNT = 0
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

def create_instances(conn):
    """
    Request spot instances.

    :type conn: boto.ec2.connection.EC2Connection
    :param conn: An EC2Connection object in which to request spot instances

    :rtype: boto.ec2.instance.Reservation
    :return: The Reservation object representing the spot instance request
    """
    return conn.request_spot_instances(price=options.price,
                                       image_id=options.ami,
                                       count=options.count,
                                       key_name=options.key,
                                       security_groups=options.sec.split(','),
                                       instance_type=options.type)

def get_instance_ids(reservation):
    """
    Get instance IDs for a particular reservation.

    :type reservation: boto.ec2.instance.Reservation
    :param reservation: A Reservation object created by requesting spot instances

    :rtype: list
    :return: A list containing strings representing the instance IDs of the
             given Reservation
    """
    request_ids = [request.id for request in reservation]
    while True:
        sleep(1)
        requests = conn.get_all_spot_instance_requests(request_ids=request_ids)
        instance_ids = []
        for request in requests:
            instance_id = request.instance_id
            #print 'instance_id is %s' % instance_id
            if instance_id is None:
                break
            #print 'appending %s' % instance_id
            instance_ids.append(instance_id)
        if len(instance_ids) < len(reservation):
            #print 'not enough instance_ids'
            continue
        break
    return instance_ids

def tag_instances(conn, instance_ids):
    """
    Attach identifying tags to the specified instances.

    :type conn: boto.ec2.connection.EC2Connection
    :param conn: An EC2Connection object in which to tag spot instances

    :type instance_ids: list
    :param instance_ids: A list of instance IDs to tag

    :rtype: boolean
    :return: A boolean indicating whether tagging was successful
    """
    tags = {'Name': options.tag}
    return conn.create_tags(instance_ids, tags)

def get_tagged_instances(conn):
    """
    Get instances labeled with the tags specified in the options.

    :type conn: boto.ec2.connection.EC2Connection
    :param conn: An EC2Connection object in which to tag spot instances

    :rtype: list
    :return: A list of strings representing the IDs of the tagged instances
    """
    filters = {'tag:Name': options.tag}
    return [instance.id for reservation in conn.get_all_instances(filters=filters)
            for instance in reservation.instances]

if __name__ == '__main__':
    conn = connect_to_region('us-west-2')

    if options.count:
        # Create spot instances
        reservation = create_instances(conn)
        # Tag created spot instances
        instance_ids = get_instance_ids(reservation)
        tag_instances(conn, instance_ids)

    # Get tagged instances
    tagged_instances = get_tagged_instances(conn)
    print tagged_instances # debug

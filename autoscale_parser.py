from boto.ec2 import connect_to_region
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from time import sleep

COUNT = 0
REGION = 'us-west-2'
PRICE = '0.300'
AMI = 'ami-f48c14c4'
KEY = 'data-extraction'
SECURITY_GROUPS = 'sshable'
INSTANCE_TYPE = 'm2.4xlarge'
TAG = 'parser'

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
(opt, args) = op.parse_args()

class EC2RegionConnection(object):
    """
    A connection to a specified EC2 region.
    """
    def __init__(self, region=opt.region):
        """
        Open a boto.ec2.connection.EC2Connection object.

        :type region: string
        :param region: A string representing the EC2 region to connect to
        """
        self.conn = connect_to_region(region)

    def _request_instances(self, count):
        """
        Request spot instances.

        :type count: int
        :param count: The number of spot instances to request

        :rtype: boto.ec2.instance.Reservation
        :return: The Reservation object representing the spot instance request
        """
        return self.conn.request_spot_instances(price=opt.price,
                                                image_id=opt.ami,
                                                count=count,
                                                key_name=opt.key,
                                                security_groups=opt.sec.split(','),
                                                instance_type=opt.type)

    def _get_instance_ids(self, reservation):
        """
        Get instance IDs for a particular reservation.

        :type reservation: boto.ec2.instance.Reservation
        :param reservation: A Reservation object created by requesting spot
                            instances

        :rtype: list
        :return: A list containing strings representing the instance IDs of the
                 given Reservation
        """
        r_ids = [request.id for request in reservation]
        while True:
            sleep(1)
            requests = self.conn.get_all_spot_instance_requests(request_ids=r_ids)
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

    def _tag_instances(self, instance_ids):
        """
        Attach identifying tags to the specified instances.

        :type instance_ids: list
        :param instance_ids: A list of instance IDs to tag

        :rtype: boolean
        :return: A boolean indicating whether tagging was successful
        """
        tags = {'Name': opt.tag}
        return self.conn.create_tags(instance_ids, tags)

    def add_instances(self, count):
        """
        Add a specified number of instances.

        :type count: int
        :param count: The number of instances to add

        :rtype: boolean
        :return: A boolean indicating whether adding instances was successful
        """
        try:
            # Create spot instances
            reservation = self._request_instances(conn)
            # Tag created spot instances
            instance_ids = self._get_instance_ids(reservation)
            self._tag_instances(conn, instance_ids)
        except:
            traceback.print_exc()
            return False
        return True

    def delete_instances(self, something): pass

    def scale(self, count):
        """
        Add or delete a number of instances depending on the polarity of the
        count parameter.

        :type count: int
        :param count: The number by which to modify the number of active instances
        """
        if count > 0:
            return self.add_instances(count)
        elif count < 0:
            return self.delete_instances(abs(count))
        else:
            return True

    def get_tagged_instances(self):
        """
        Get instances labeled with the tags specified in the options.

        :rtype: list
        :return: A list of strings representing the IDs of the tagged instances
        """
        filters = {'tag:Name': opt.tag}
        return [instance.id for reservation in
                self.conn.get_all_instances(filters=filters) for instance in
                reservation.instances]

if __name__ == '__main__':
    c = EC2RegionConnection()

    if opt.count:
        # TODO: Write function that ensures the # of active instances == opt.count
        difference = opt.count - len(c.get_tagged_instances)
        if difference > 0:
            c.add_instances(difference)
        else:
            pass # TODO

    # Get tagged instances
    tagged_instances = get_tagged_instances(conn)
    print tagged_instances # debug

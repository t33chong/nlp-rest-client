from boto.ec2.autoscale import connect_to_region as connect_autoscale_to
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import ScalingPolicy

from optparse import OptionParser

AMI = 'ami-dee37dee'
GROUP_NAME = 'parser'
DEFAULT_MIN = 4
DEFAULT_MAX = 20

parser = OptionParser()
parser.add_option('-m', '--max', dest='max', default=None,
                  help='Update the max number of instances for the group')
parser.add_option('-n', '--min', dest='min', default=None,
                  help='Update the min number of instances for the group')
parser.add_option('-d', '--desired', dest='desired', default=None,
                  help='Change the desired capacity of the group')
parser.add_option('-g', '--group', dest='group', default=GROUP_NAME,
                  help='The autoscale group name to operate over')
parser.add_option('-a', '--ami', dest='ami', default=AMI,
                  help='The AMI to use, if creating a group')
parser.add_option('-b', '--rebuild', dest='rebuild', action='store_true', default=False,
                  help='Whether to rebuild the group (deletes the old group)')
parser.add_option('-r', '--region', dest='region', action='store_true', default='us-west-2',
                  help='Amazon region to connect to')
parser.add_option('-z', '--zones', dest='zones', action='store_true', default='us-west-2',
                  help="Availability zones for this autoscale group")
parser.add_option('-c', '--create', dest='create', action='store_true', default=False,
                  help='Whether to create the group for the first time')
parser.add_option('-x', '--destroy', dest='destroy', action='store_true', default=False,
                  help='Whether to destroy the group without rebuilding')
parser.add_option('-y', '--delete', dest='destroy', action='store_true', default=False,
                  help='Whether to destroy the group without rebuilding')

(options, args) = parser.parse_args()

conn = connect_autoscale_to(options.region)
lcname = options.group+'_config'
groups = filter(lambda x:x.name == GROUP_NAME, conn.get_all_groups())
group = groups[0] if groups else None

def create_group():
    global conn, lcname

    lc = LaunchConfiguration(name=lcname,
                             image_id=options.ami,
                             key_name='relwellnlp',
                             instance_type='m2.4xlarge',
                             security_groups=['sshable'])

    conn.create_launch_configuration(lc)

    min = options.min if options.min is not None else DEFAULT_MIN
    max = options.max if options.max is not None else DEFAULT_MAX
    group = AutoScalingGroup(group_name=options.group,
                             availability_zones=options.zones.split(','),
                             launch_config=lc,
                             min_size=min,
                             max_size=max,
                             connection=conn)

    conn.create_auto_scaling_group(group)

    scale_up_policy = ScalingPolicy(
            name='scale_up', adjustment_type='ChangeInCapacity',
            as_name=options.group, scaling_adjustment=1, cooldown=180)

    scale_down_policy = ScalingPolicy(
            name='scale_down', adjustment_type='ChangeInCapacity',
            as_name=options.group, scaling_adjustment=-1, cooldown=180)

    conn.create_scaling_policy(scale_up_policy)
    conn.create_scaling_policy(scale_down_policy)



if group and options.rebuild or options.destroy:
    map(lambda x: x.delete(), conn.get_all_policies(options.group))
    try:
        group.shutdown_instances()
    except:
        pass
    conn.delete_auto_scaling_group(options.group, force_delete=True)

if options.rebuild or options.destroy:
    conn.delete_launch_configuration(lcname) 

if options.create or options.rebuild:
    create_group()

if not options.create and not options.rebuild and not options.destroy:
    # we're just modifying a group
    happened = False
    if options.desired:
        happened = True
        group.set_capacity(options.desired)
    
    if options.min:
        happened = True
        group.min_size = options.min

    if options.max:
        happened = True
        group.max_size = options.max

    if happened:
        group.update()
    else:
        print "Nothing happened. Use --help to figure out how to use this script."

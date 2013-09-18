from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import ScalingPolicy
from boto.ec2.cloudwatch import MetricAlarm
from boto import connect_cloudwatch

from optparse import OptionParser

AMI = 'ami-eafd62da'
GROUP_NAME = 'parser_puller'
DEFAULT_MIN = 4
DEFAULT_MAX = 25

parser = OptionParser()
parser.add_option('-m', '--max', dest='max', default=None,
                  'Update the max number of instances for the group')
parser.add_option('-n', '--min', dest='min', default=None,
                  'Update the min number of instances for the group')
parser.add_option('-d', '--desired', dest='desired', default=None,
                  'Change the desired capacity of the group')
parser.add_option('-g', '--group', dest='group', default=GROUP_NAME,
                  'The autoscale group name to operate over')
parser.add_option('a', '--ami', dest='ami', default=AMI,
                  'The AMI to use, if creating a group')
(options, args) = parser.parser_args()

conn = AutoScaleConnection()
groups = filter(lambda x:group.name == GROUP_NAME, connection.get_all_groups())
group = groups[0] if groups else None

if group is None:
    #oh, looks like we're creating this group
    lc = LaunchConfiguration(name=options.group+'_config',
                             image_id=AMI,
                             key_name='relwellnlp',
                             security_groups=['sshable'])

    conn.create_launch_configuration(lc)

    min = options.min if options.min is not None else DEFAULT_MIN
    max = options.max if options.max is not None else DEFAULT_MAX
    group = AutoScalingGroup(group_name=option.group,
                             availability_zones=['us-west-1']
                             launch_config=lc,
                             min_size=min,
                             max_size=max,
                             connection=conn)

    scale_up_policy = ScalingPolicy(
            name='scale_up', adjustment_type='ChangeInCapacity',
            as_name='my_group', scaling_adjustment=1, cooldown=180)

    scale_down_policy = ScalingPolicy(
            name='scale_down', adjustment_type='ChangeInCapacity',
            as_name='my_group', scaling_adjustment=-1, cooldown=180)

    conn.create_scaling_policy(scale_up_policy)
    conn.create_scaling_policy(scale_down_policy)

    cloudwatch = connect_cloudwatch()
    alarm_dimensions = {"AutoScalingGroupName": option.group}
    scale_up_alarm = MetricAlarm(
            name='scale_up_on_cpu', namespace='AWS/EC2',
            metric='CPUUtilization', statistic='Average',
            comparison='>', threshold='70',
            period='60', evaluation_periods=2,
            alarm_actions=[scale_up_policy.policy_arn],
            dimensions=alarm_dimensions)
    cloudwatch.create_alarm(scale_up_alarm)

    scale_down_alarm = MetricAlarm(
            name='scale_down_on_cpu', namespace='AWS/EC2',
            metric='CPUUtilization', statistic='Average',
            comparison='<', threshold='40',
            period='60', evaluation_periods=2,
            alarm_actions=[scale_down_policy.policy_arn],
            dimensions=alarm_dimensions)

    cloudwatch.create_alarm(scale_down_alarm)
else:
    #okay, the group already exists, so apply all desired changes
    if options.desired:
        group.set_capacity(options.desired)
    
    if options.min:
        group.min_size = options.min

    if options.max:
        group.max_size = options.max

    group.update()

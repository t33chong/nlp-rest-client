from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import ScalingPolicy
from boto.ec2.cloudwatch import MetricAlarm
from boto import connect_cloudwatch

AMI = 'ami-eafd62da'
GROUP_NAME = 'parser_pullers'

conn = AutoScaleConnection()

groups = connection.get_all_groups()

if GROUP_NAME not in [group.name for group in groups]:
    lc = LaunchConfiguration(name='parser_puller_config',
                             image_id=AMI,
                             key_name='relwellnlp',
                             security_groups=['sshable'])

    conn.create_launch_configuration(lc)

    group = AutoScalingGroup(group_name='parser_pullers',
                             availability_zones=['us-west-1']
                             launch_config=lc,
                             min_size=4,
                             max_size=25,
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
    alarm_dimensions = {"AutoScalingGroupName": 'parser_pullers'}
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

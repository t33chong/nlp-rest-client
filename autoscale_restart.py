from boto.ec2.autoscale import connect_to_region
from boto.ec2 import connect_to_region as connect_ec2
from boto.manage.cmdshell import sshclient_from_instance

autoscale = connect_to_region('us-west-2')
group = autoscale.get_all_groups(names=['parser'])[0]
ec2 = connect_ec2('us-west-2')
instances = ec2.get_all_instances(instance_ids=[instance.instance_id for instance in group.instances])

def restart_service(instance):
    ssh_client = sshclient_from_instance(instance, '/home/robert/relwellnlp.pem', user_name='ubuntu')
    ssh_client.known_hosts = None
    commands = ['sudo sv stop parser_daemon', 'sudo killall java', 'sudo sv start parser_daemon', 'sudo tail /var/log/runit/parser_poller/current']
    results = map(lambda x: ssh_client.run(x), commands)
    print results

map(restart_service, [reservation.instances[0] for reservation in instances])

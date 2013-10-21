from boto.ec2.autoscale import connect_to_region
from boto.ec2 import connect_to_region as connect_ec2
from boto.manage.cmdshell import sshclient_from_instance

ec2 = connect_ec2('us-west-2')
instances = ec2.get_all_instances()

def restart_service(instance):
    try:
        ssh_client = sshclient_from_instance(instance, '/home/robert/relwellnlp.pem', user_name='ubuntu')
        ssh_client.known_hosts = None
        commands = ['cd /home/ubuntu/nlp-rest-client && git pull --rebase origin master', 'sudo sv restart wiki-data']
        results = map(lambda x: ssh_client.run(x), commands)
        print results
    except:
        pass

map(restart_service, [reservation.instances[0] for reservation in instances])

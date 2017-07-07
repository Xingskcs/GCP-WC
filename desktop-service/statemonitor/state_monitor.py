"""Listens to running events.

Monitor the state of running containers.
"""

import os
import time
import yaml
import glob
import docker
import socket
import kazoo
import logging.config
from kazoo.client import KazooClient

import sys
sys.path.append("..")
import fs
import appenv
import zkutils
import zknamespace as z
from appevents import appevents
from apptrace import events

#logging
logging.basicConfig(filename = os.path.join("../../log", 'state_monitor.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


class StateMonitor(object):
    """Monitor the state of running containers"""

    __slots__ = (
        'tm_env',
        'zk'
    )

    def __init__(self, root, zk):
        logging.info('init state monitor: %s', root)
        self.tm_env = appenv.WindowsAppEnvironment(root = root)
        self.zk = zk

    def _run_real(self, client, zk):
        """Monitor the state of running containers

        running containers: get ids from ../running
        running->exited

        if exited code equals 0, finished
        else, aborted and so on.
        if finished, docker rm container and state change to deleted.
        """
        running_containers = {}
        while True:
            running_apps = {
                os.path.basename(manifest)
                for manifest in glob.glob(os.path.join(self.tm_env.running_dir, '*'))
            }
            for app in running_apps:
                with open(os.path.join(self.tm_env.running_dir, app)) as f:
                    manifest_data = yaml.load(stream=f)
                running_containers[manifest_data['container_id']] = app

            #for every container, monitor if exit
            exited_containers = set()
            for exited_container in client.containers.list(all,filters = {"status":"exited"}):
                exited_containers.add(exited_container.id)

            finished_containers = set()
            for finished_container in client.containers.list(all,filters = {"exited":"0"}):
                finished_containers.add(finished_container.id)

            killed_containers = set()
            for killed_container in client.containers.list(all, filters = {"exited":"137"}):
                killed_containers.add(killed_container.id)

            aborted_containers = {}
            for exited_code in range(1, 256):
                if(exited_code != 137):
                    for container in client.containers.list(all, filters = {"exited": str(exited_code)}):
                        aborted_containers[container.id] = exited_code

            for container_id in running_containers:
                #if container_id not in recorded_containers:
                #check whether exited
                if container_id in exited_containers:
                    instance_name = running_containers.get(container_id)
                    _HOSTNAME = socket.gethostname()
                    with open(os.path.join(self.tm_env.cache_dir, instance_name)) as f:
                        manifest_data = yaml.load(stream=f)
                    service = manifest_data['services'][0]['name']

                    # if container is normally finished
                    if container_id in finished_containers:
                        #create exited node
                        logging.info("exited: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.ServiceExitedTraceEvent(
                                instanceid=instance_name,
                                uniqueid=container_id,
                                service=manifest_data['services'][0]['name'],
                                rc='0',
                                signal='0'
                            )
                        )
                        #create finished node
                        logging.info("finished: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.FinishedTraceEvent(
                                instanceid=instance_name,
                                rc='0',
                                signal='0',
                                payload=''
                            )
                        )
                        #create deleted node
                        logging.info("delete: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.DeletedTraceEvent(
                                instanceid=instance_name
                            )
                        )
                        zkutils.ensure_deleted(zk, z.path.scheduled(instance_name))
                        zkutils.ensure_deleted(zk, z.path.placement(_HOSTNAME+'/'+instance_name))
                        client.containers.get(container_id).remove()

                    #if container is killed
                    elif container_id in killed_containers:
                        #create exited node
                        logging.info("exited: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.ServiceExitedTraceEvent(
                                instanceid=instance_name,
                                uniqueid=container_id,
                                service=manifest_data['services'][0]['name'],
                                rc='137',
                                signal='137'
                            )
                        )
                        #create killed node
                        logging.info("killed: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.KilledTraceEvent(
                                instanceid=instance_name,
                                #is_oom=bool(exitinfo.get('oom')),
                                is_oom = False,
                            )
                        )
                        #create deleted node
                        logging.info("delete: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.DeletedTraceEvent(
                                instanceid=instance_name
                            )
                        )
                        #zkutils.ensure_deleted(zk, z.path.scheduled(instance_name))
                        zkutils.ensure_deleted(zk, z.path.placement(_HOSTNAME + '/' + instance_name))
                        client.containers.get(container_id).remove()

                    #if container is aborted
                    else:
                        #create exited node
                        logging.info("exited: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.ServiceExitedTraceEvent(
                                instanceid=instance_name,
                                uniqueid=container_id,
                                service=manifest_data['services'][0]['name'],
                                rc=str(aborted_containers.get(container_id)),
                                signal=str(aborted_containers.get(container_id))
                            )
                        )
                        #create aborted node
                        logging.info("aborted: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.AbortedTraceEvent(
                                why=str(aborted_containers.get(container_id)),
                                instanceid=instance_name,
                                payload=None
                            )
                        )
                        #create deleted node
                        logging.info("delete: %s", running_containers.get(container_id))
                        appevents.post(
                            self.tm_env.app_events_dir,
                            events.DeletedTraceEvent(
                                instanceid=instance_name
                            )
                        )
                        # zkutils.ensure_deleted(zk, z.path.scheduled(instance_name))
                        zkutils.ensure_deleted(zk, z.path.placement(_HOSTNAME + '/' + instance_name))
                        client.containers.get(container_id).remove()

                    fs.rm_safe(os.path.join(self.tm_env.running_dir, running_containers.get(container_id)))
                    logging.info("delete running file: %s", running_containers.get(container_id))
            time.sleep(1)

    def run(self):
        client = docker.from_env()
        self._run_real(client, self.zk)

root = os.path.abspath('../..')
master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()
stateMonitorService = StateMonitor(root, zk)
stateMonitorService.run()
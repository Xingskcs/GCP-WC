"""Listens to cache events.

Applications that are scheduled to run on the server are mirrored in the
cache directory

Configure and running the apps.
"""
import os
import glob
import docker
import time
import socket
import logging.config
import yaml
import kazoo
import tempfile
from kazoo.client import KazooClient

import sys
sys.path.append("..")
import appenv
import dirwatch
import appcfg
import zkutils
import zknamespace as z
from appevents import appevents
from apptrace import events

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'appcfgmgr.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_SERVERS_ACL = zkutils.make_role_acl('servers', 'rwcd')
_HEARTBEAT_SEC = 30

class AppCfgMgr(object):
    """Configure apps from the cache onto the desktop"""

    __slots__ = (
        'tm_env',
        '_is_active',
        'zk'
    )

    def __init__(self, root, zk):
        logging.info('init appcfgmgr: %s',root)
        self.tm_env = appenv.WindowsAppEnvironment(root = root)
        self._is_active = False
        self.zk = zk

    def run(self):
        """Setup directories' watches and start the re-scan ticker.
                """
        # Start idle
        self._is_active = True

        watch = dirwatch.DirWatcher(self.tm_env.cache_dir)
        watch.on_created = self._on_created
        watch.on_modified = self._on_modified
        watch.on_deleted = self._on_deleted

        while True:
            if watch.wait_for_events(timeout=_HEARTBEAT_SEC):
                watch.process_events(max_events=5)
            else:
                if self._is_active is True:
                    cached_files = glob.glob(
                        os.path.join(self.tm_env.cache_dir, '*')
                    )
                    running_links = glob.glob(
                        os.path.join(self.tm_env.running_dir, '*')
                    )
                    # Calculate the container names from every event file
                    cached_containers = {
                        appcfg.eventfile_unique_name(filename)
                        for filename in cached_files
                    }
                    # Calculate the instance names from every event running
                    # link
                    running_instances = {
                        os.path.basename(linkname)
                        for linkname in running_links
                    }
                    logging.info('content of %r and %r: %r <-> %r',
                                  self.tm_env.cache_dir,
                                  self.tm_env.running_dir,
                                  cached_containers,
                                  running_instances)

                else:
                    logging.info('Still inactive during heartbeat event.')
        logging.info('service shutdown.')

    def _on_created(self, event_file):
        """Handle a new cached manifest event: configure an instance.

        :param event_file:
             Full path to an event file
        :type event_file:
            ``str``
        """
        instance_name = os.path.basename(event_file)

        if instance_name == '.seen':
            self._first_sync()
            return

        elif instance_name[0] == '.':
            #Ignore all dot files
            return
        elif self._is_active is False:
            #Ingnore all created events while we are not running
            logging.info("Inactive in created event handler.")
            return
        elif os.path.islink(os.path.join(self.tm_env.running_dir,
                                         instance_name)):
            logging.info("Event on alreadly configured %r",
                  instance_name)
            return
        elif self._configure(instance_name):
            self._refresh_supervisor(instance_names=[instance_name])

        pass

    def _on_modified(self, event_file):
        pass

    def _on_deleted(self, event_file):
        pass

    def _first_sync(self):
        """Bring the appcfgmgr into active mode and do a first sync.
        """
        if self._is_active is not True:
            logging.info("Cache folder ready. Processing events.")
            self._is_active = True
            self._synchronize()

    def _synchronize(self):
        """Synchronize cache/ instances with running/ instances.

        We need to revalidate three things:

          - All running instances must have an equivalent entry in cache or be
            terminated.

          - All event files in the cache that do not have running link must be
            started.

          - The cached entry and the running link must be for the same
            container (equal unique name). Otherwise, terminate it.

        """
        cached_files = glob.glob(os.path.join(self.tm_env.cache_dir, '*'))
        running_files = glob.glob(os.path.join(self.tm_env.running_dir, '*'))

        #configure that should to be configured
        for file_name in cached_files - running_files:
            self._configure(file_name)



    def _configure(self, instance_name):
        """Configures and starts the instance based on instance cached event.

        :param ``str`` instance_name:
            Name of the instance to configure
        :returns ``bool``:
            True for successfully configured container.
        """
        time.sleep(3)
        event_file = os.path.join(
            self.tm_env.cache_dir,
            instance_name
        )

        logging.info("configuring %s", instance_name)
        with open(event_file) as f:
            manifest_data = yaml.load(stream=f)
        client = docker.from_env()
        docker_container = client.containers.create(image = 'resource',
                                                    command = manifest_data['services'][0]['command'])

        if docker_container in client.containers.list(all):
            # eventtime = time.time()
            # _HOSTNAME = socket.gethostname()
            # event = 'configured'
            # data = ''
            # eventnode = '%s,%s,%s,%s' % (eventtime, _HOSTNAME, event, data)
            # try:
            #     self.zk.create(path = z.path.task(instance_name, eventnode))
            # except kazoo.client.NodeExistsError:
            #     logging.info(' creare node error')
            # logging.info("configure success %s", instance_name)

            appevents.post(
                self.tm_env.app_events_dir,
                events.ConfiguredTraceEvent(
                    instanceid=instance_name,
                    uniqueid=docker_container.id
                )
            )
            logging.info("configure success %s", instance_name)


        logging.info("starting %s", instance_name)
        canstarted = True
        try:
            docker_container.start()
        except docker.errors.APIError:
            canstarted = False
        if canstarted:
            manifest_file = os.path.join(self.tm_env.running_dir, instance_name)
            manifest = {'container_id':docker_container.id}
            if not os.path.exists(manifest_file):
                with tempfile.NamedTemporaryFile(dir=self.tm_env.running_dir,
                                                 prefix='.%s-' % instance_name,
                                                 delete=False,
                                                 mode='w') as temp_manifest:
                    yaml.dump(manifest, stream=temp_manifest)
                os.rename(temp_manifest.name, manifest_file)
            logging.info('Created running manifest: %s', manifest_file)

            # eventtime = time.time()
            # _HOSTNAME = socket.gethostname()
            # event = 'service_running'
            # data = ''
            # eventnode = '%s,%s,%s,%s' % (eventtime, _HOSTNAME, event, data)
            # try:
            #     self.zk.create(path = z.path.task(instance_name, eventnode))
            # except kazoo.client.NodeExistsError:
            #     logging.info(' creare node error')

            appevents.post(
                self.tm_env.app_events_dir,
                events.ServiceRunningTraceEvent(
                    instanceid=instance_name,
                    uniqueid=docker_container.id,
                    service=manifest_data['services'][0]['name']
                )
            )
            logging.info("running %s", instance_name)

#root = os.path.abspath('../..')
root = 'C:/tmp'
master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()
appcfgmgrService = AppCfgMgr(root, zk)
appcfgmgrService.run()


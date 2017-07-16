"""Desktop cleanup service.
"""

import os
import glob
import yaml
import time
import socket
import docker
import logging.config
from kazoo.client import KazooClient

import sys
sys.path.append("..")
import appenv
import dirwatch
import zkutils
import zknamespace as z
import fs

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'cleanup.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_HOSTNAME = socket.gethostname()
class CleanUp(object):
    """Monitor the cleanup dir."""

    __slots__ = (
        'tm_env',
        'zk',
        '_is_active'
    )

    def __init__(self, root, zk):
        logging.info('init state monitor: %s', root)
        self.tm_env = appenv.WindowsAppEnvironment(root=root)
        self.zk = zk
        self._is_active = False

    def run(self):
        """Setup directories' watches and start the re-scan ticker.
                        """
        # Start idle
        self._is_active = True

        while True:
            if self._is_active is True:
                cleanup_files = glob.glob(
                    os.path.join(self.tm_env.cleanup_dir, '*')
                )
                logging.info('content of %r : %r',
                                self.tm_env.cleanup_dir,
                                cleanup_files)
                for cleanup_file in cleanup_files:
                    self._cleanup(cleanup_file)
            else:
                logging.info('Still inactive during heartbeat event.')
            time.sleep(2)

    def _cleanup(self, event_file):
        """Handle a new cleanup event: cleanup a container.

        :param event_file:
             Full path to an event file
        :type event_file:
            ``str``
        """
        instance_name = os.path.basename(event_file)

        if self._is_active is False:
            #Ingnore all cleanup events while we are not running
            logging.info("Inactive in cleanup event handler.")
            return
        else:
            logging.info("cleanup: %s", instance_name)
            zkutils.ensure_deleted(zk, z.path.placement(_HOSTNAME + '/' + instance_name))
            client = docker.from_env()
            with open(os.path.join(self.tm_env.cleanup_dir, instance_name)) as f:
                manifest_data = yaml.load(stream=f)
            client.containers.get(manifest_data['container_id']).remove()
            fs.rm_safe(event_file)
        pass


root = 'C:/tmp'
master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()
cleanupService = CleanUp(root, zk)
cleanupService.run()
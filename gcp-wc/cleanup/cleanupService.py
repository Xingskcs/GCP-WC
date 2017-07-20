"""Desktop cleanup service.

"""
import os
import time
import glob
import yaml
import errno
import socket
import docker
import functools
import collections
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'cleanupSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

PLACEMENT = '/placement'
RUNNING = '/running'

CACHE_DIR = 'cache'
RUNNING_DIR = 'running'
CLEANUP_DIR = 'cleanup'

_HOSTNAME = socket.gethostname()

class CleanupSvc (win32serviceutil.ServiceFramework):
    """Register Zookeeper Service"""

    _svc_name_ = "CleanupService"
    _svc_display_name_ = "CleanupService"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self.root = 'C:/tmp'
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        master_hosts = '192.168.1.119:2181'
        zk = KazooClient(hosts = master_hosts)
        zk.start()
        client = docker.from_env()
        while True:
            cleanup_files = glob.glob(
                os.path.join(os.path.join(self.root, CLEANUP_DIR), '*')
            )
            logging.info('content of %r : %r',
                         os.path.join(self.root, CLEANUP_DIR),
                         cleanup_files)
            for cleanup_file in cleanup_files:
                self._cleanup(zk, client, cleanup_file)
            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break

    def _cleanup(self, zk, client, event_file):
        """Handle a new cleanup event: cleanup a container.

        :param event_file:
             Full path to an event file
        :type event_file:
            ``str``
        """
        instance_name = os.path.basename(event_file)

        logging.info("cleanup: %s", instance_name)
        if zk.exists(path.placement(_HOSTNAME + '/' + instance_name)):
            zk.delete(path.placement(_HOSTNAME + '/' + instance_name))
        rm_safe(os.path.join(os.path.join(self.root, CACHE_DIR), instance_name))
        with open(os.path.join(os.path.join(self.root, CLEANUP_DIR), instance_name)) as f:
            manifest_data = yaml.load(stream=f)
        client.containers.get(manifest_data['container_id']).remove()
        rm_safe(os.path.join(os.path.join(self.root, RUNNING_DIR), instance_name))
        rm_safe(event_file)
        pass

def rm_safe(path):
    """Removes file, ignoring the error if file does not exist."""
    try:
        os.unlink(path)
    except OSError as err:
        # If the file does not exists, it is not an error
        if err.errno == errno.ENOENT:
            pass
        else:
            raise

def join_zookeeper_path(root, *child):
    """"Returns zookeeper path joined by slash."""
    return '/'.join((root,) + child)


def make_path_f(zkpath):
    """"Return closure that will construct node path."""
    return staticmethod(functools.partial(join_zookeeper_path, zkpath))


path = collections.namedtuple('path', """
    running
    placement
    """)

path.placement = make_path_f(PLACEMENT)
path.running = make_path_f(RUNNING)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(CleanupSvc)
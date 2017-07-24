"""Register Zookeeper Service.

When the screen is locked, register zookeeper.
"""
import os
import glob
import yaml
import docker
import socket
import collections
import functools
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'registerZookeeperSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

SERVERS = '/servers'
SERVER_PRESENCE = '/server.presence'
BLACKEDOUT_SERVERS = '/blackedout.servers'

screen_state_file = 'screen_state.txt'

_HOSTNAME = socket.gethostname()
RUNNING_DIR = 'running'
CLEANUP_DIR = 'cleanup'

class RegisterZookeeperSvc (win32serviceutil.ServiceFramework):
    """Register Zookeeper Service"""

    _svc_name_ = "RegisterZookeeperService"
    _svc_display_name_ = "RegisterService"

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
            f = open(os.path.join(self.root, screen_state_file), 'r')
            screen_state = f.read()
            #if screen_state == 'Lock':
            if True:
                node_data = zk.get(path.server('node'))
                # For desktop, we add a 'windows' label, in order to schedule better later.
                desktop_data = node_data[0].decode().replace('~', 'windows', 1)
                if not zk.exists(path.server(_HOSTNAME)):
                    zk.create(path.server(_HOSTNAME), desktop_data.encode('utf-8'))
                    logging.info("Create servers node: %s", _HOSTNAME)
                elif not zk.exists(path.server_presence(_HOSTNAME)):
                    zk.create(path.server_presence(_HOSTNAME), desktop_data.encode('utf-8'), ephemeral=True)
                    logging.info("Create server.presence node: %s", _HOSTNAME)
            else:
                if zk.exists(path.server_presence(_HOSTNAME)):
                    zk.delete(path.server_presence(_HOSTNAME))
                    logging.info("Delete server.presence node: %s", _HOSTNAME)
            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break

def join_zookeeper_path(root, *child):
    """"Returns zookeeper path joined by slash."""
    return '/'.join((root,) + child)


def make_path_f(zkpath):
    """"Return closure that will construct node path."""
    return staticmethod(functools.partial(join_zookeeper_path, zkpath))


path = collections.namedtuple('path', """
    server_presence
    blackedout_server
    server
    """)

path.server_presence = make_path_f(SERVER_PRESENCE)
path.server = make_path_f(SERVERS)
path.blackedout_server = make_path_f(BLACKEDOUT_SERVERS)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(RegisterZookeeperSvc)
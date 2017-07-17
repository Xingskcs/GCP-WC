"""Update Resources Service.

Update the resources of desktop periodly.
"""
import os
import socket
import psutil
import collections
import functools
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'updateResourcesSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

SERVERS = '/servers'
SERVER_PRESENCE = '/server.presence'

_HOSTNAME = socket.gethostname()

class UpdateResourcesSvc (win32serviceutil.ServiceFramework):
    """Register Zookeeper Service"""

    _svc_name_ = "UpdateResourcesService"
    _svc_display_name_ = "UpdateResourcesService"

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
        node_data = zk.get(path.server('node'))
        # For desktop, we add a 'windows' label, in order to schedule better later.
        desktop_data = node_data[0].decode().replace('~', 'windows', 1)
        while True:
            # update info
            remain_cpu, remain_mem, remain_disk = monitorResources()
            update_info = 'cpu: {cpuinfo}%\ndisk: {diskinfo}M\nlabel: windows\nmemory: {meminfo}M\n'.format(
                cpuinfo=remain_cpu, diskinfo=remain_disk, meminfo=remain_mem)
            desktop_data = update_info + desktop_data[desktop_data.find('parent'):]
            if zk.exists(path.server(_HOSTNAME)):
                zk.set(path.server(_HOSTNAME), desktop_data.encode('utf-8'))
                logging.info("Update resources infomation %s", _HOSTNAME)
            if zk.exists(path.server_presence(_HOSTNAME)):
                zk.set(path.server_presence(_HOSTNAME), desktop_data.encode('utf-8'))
                logging.info("Update resources infomation %s", _HOSTNAME)
            if win32event.WaitForSingleObject(self.hWaitStop, 60000) == win32event.WAIT_OBJECT_0:
                break

def monitorResources(interval=1.0):
    """Monitor windows desktop's resources useage


    Args:
        interval:float.The interval(s) between monitor.interval = 1.0 equals 1HZ

    Returns:
        False:monitor throw an exception or cause an error at any time.
        remain_cpu
        remain_mem
        remain_disk
    """
    remain_cpu = int(100-psutil.cpu_percent(interval))
    remain_mem = int(psutil.virtual_memory().available/1024/1024)
    remain_disk = int(psutil.disk_usage('/').free/1024/1024)
    return remain_cpu, remain_mem, remain_disk

def join_zookeeper_path(root, *child):
    """"Returns zookeeper path joined by slash."""
    return '/'.join((root,) + child)


def make_path_f(zkpath):
    """"Return closure that will construct node path."""
    return staticmethod(functools.partial(join_zookeeper_path, zkpath))


path = collections.namedtuple('path', """
    server_presence
    server
    """)

path.server_presence = make_path_f(SERVER_PRESENCE)
path.server = make_path_f(SERVERS)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(UpdateResourcesSvc)
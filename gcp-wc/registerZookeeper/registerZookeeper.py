"""Register desktop in zookeeper"""

#Now the desktop is always up in zookeeper.
#Time and screen state should be considered later.

import socket
import logging.config
import os
import time

from kazoo.client import KazooClient

import monitorResources
import sys
sys.path.append("..")
import zknamespace as z

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'registerZookeeper.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_HOSTNAME = socket.gethostname()

class RegisterZookeeperService(object):

    __slots__ = (
        'start_time',
        'end_time',
        'zk',
    )

    def __init__(self, zk):
        logging.info('init register zookeeper.')
        self.zk = zk

    def run(self):
        while True:
            f = open('screen_state.txt', 'r')
            screen_state = f.read()
            if screen_state == 'Lock':
                node_data = self.zk.get(z.path.server('node'))
                #For desktop, we add a 'windows' label, in order to schedule better later.
                desktop_data = node_data[0].decode().replace('~','windows',1)
                #update cpu info
                remain_cpu, remain_mem, remain_disk = monitorResources.monitorResources()
                print(remain_cpu)
                print(remain_mem)
                print(remain_disk)
                update_info='cpu: {cpuinfo}%\ndisk: {diskinfo}M\nlabel: windows\nmemory: {meminfo}M\n'.format(
                            cpuinfo = remain_cpu, diskinfo = remain_disk, meminfo = remain_mem)
                print(update_info)
                desktop_data = update_info+desktop_data[desktop_data.find('parent'):]
                if not self.zk.exists(z.path.server(_HOSTNAME)):
                    self.zk.create(z.path.server(_HOSTNAME), desktop_data.encode('utf-8'))
                    logging.info("Create servers node: %s", _HOSTNAME)
                else:
                    self.zk.set(z.path.server(_HOSTNAME), desktop_data.encode('utf-8'))
                    logging.info("Update resources infomation %s", _HOSTNAME)
                if not self.zk.exists(z.path.server_presence(_HOSTNAME)):
                    self.zk.create(z.path.server_presence(_HOSTNAME), desktop_data.encode('utf-8'))
                    logging.info("Create server.presence node: %s", _HOSTNAME)
                else:
                    self.zk.set(z.path.server_presence(_HOSTNAME), desktop_data.encode('utf-8'))
                    logging.info("Update resources infomation %s", _HOSTNAME)

            else:
                if self.zk.exists(z.path.server_presence(_HOSTNAME)):
                    self.zk.delete(z.path.server_presence(_HOSTNAME))
                    logging.info("Delete server.presence node: %s", _HOSTNAME)
            time.sleep(1)

master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()
registerZookeeperService = RegisterZookeeperService(zk)
registerZookeeperService.run()
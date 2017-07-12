"""Register desktop in zookeeper"""

#Now the desktop is always up in zookeeper.
#Time and screen state should be considered later.

import socket
import logging.config
import os

from kazoo.client import KazooClient

import sys
sys.path.append("..")
import zknamespace as z

#logging
logging.basicConfig(filename = os.path.join("../../log", 'registerZookeeper.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


class RegisterZookeeperService(object):

    __slots__ = (
        'start_time',
        'end_time',
        'screen_state',
        'zk'
    )

    def __init__(self, zk):
        logging.info('init register zookeeper.')
        self.zk = zk

    def run(self):

        node_data = self.zk.get(z.path.server('node'))
        #For desktop, we add a 'windows' label, in order to schedule better later.
        desktop_data = node_data[0].decode().replace('~','windows',1)
        _HOSTNAME = socket.gethostname()
        if not self.zk.exists(z.path.server(_HOSTNAME)):
            self.zk.create(z.path.server(_HOSTNAME), desktop_data.encode('utf-8'))
            logging.info("Create servers node: %s", _HOSTNAME)
        if not self.zk.exists(z.path.server_presence(_HOSTNAME)):
            self.zk.create(z.path.server_presence(_HOSTNAME), desktop_data.encode('utf-8'))
            logging.info("Create server.presence node: %s", _HOSTNAME)

master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()
registerZookeeperService = RegisterZookeeperService(zk)
registerZookeeperService.run()
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KeeperState
master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()

@zk.add_listener
def watch_for_ro(state):
    if state == KazooState.CONNECTED:
        print(1)
    else:
        print(2)

while True:
    print(zk.state)

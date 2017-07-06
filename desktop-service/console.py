"""Desktop Service console entry point """

import os
from multiprocessing import Pool
#import parallelTestModule

from kazoo.client import KazooClient

from registerZookeeper import registerZookeeper
from appcfgmgr import appcfgmgr
from event_daemon import event_daemon
from statemonitor import state_monitor

def start_register_zookeeper(zk):
    registerZookeeperService = registerZookeeper.RegisterZookeeperService(zk)
    registerZookeeperService.run()


def start_event_daemon(root, zk):
    eventDaemonService = event_daemon.EventMgr(root, zk)
    eventDaemonService.run()


def start_appcfgmgr(root, zk):
    appcfgmgrService = appcfgmgr.AppCfgMgr(root, zk)
    appcfgmgrService.run()

def start_state_monitor(root, zk):
    stateMonitorService = state_monitor.StateMonitor(root, zk)
    stateMonitorService.run()

if __name__ == '__main__':
# def run(root = os.path.abspath('.')):
    """Start desktop services"""
    # extractor = parallelTestModule.ParallelExtractor()
    # extractor.runInParallel(numProcesses=1, numThreads=4)

    master_hosts = '192.168.1.119:2181'
    zk = KazooClient(hosts = master_hosts)
    zk.start()

    p = Pool(1)
    p.apply_async(start_register_zookeeper, args=(zk, ))
    # p.apply_async(start_event_daemon, args=(root, zk, ))
    # p.apply_async(start_appcfgmgr, args=(root, zk, ))
    # p.apply_async(start_state_monitor, args=(root, zk, ))

    p.close()
    p.join()




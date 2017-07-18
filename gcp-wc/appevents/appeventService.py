"""Appevent Service.

"""
import os
import glob
import kazoo
import socket
import functools
import collections
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'appeventsSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

TASKS = '/tasks'
SCHEDULED = '/scheduled'

APP_EVENTS_DIR = 'appevents'

_HOSTNAME = socket.gethostname()


class AppeventSvc (win32serviceutil.ServiceFramework):
    """Appevent Service"""

    _svc_name_ = "AppeventService"
    _svc_display_name_ = "AppeventService"

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
        while True:
            post_files = glob.glob(
                os.path.join(os.path.join(self.root, APP_EVENTS_DIR), '*')
            )
            logging.info('content of %r : %r',
                         os.path.join(self.root, APP_EVENTS_DIR),
                         post_files)
            for post_file in post_files:
                self._post(zk, post_file)

            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break

    def _post(self, zk, path):
        localpath = os.path.basename(path)

        logging.info("post: %s", localpath)
        eventtime, appname, event, data = localpath.split(',', 4)
        with open(path, mode='rb') as f:
            eventnode = '%s,%s,%s,%s' % (eventtime, _HOSTNAME, event, data)
            logging.info('Creating %s', task_path(appname, eventnode))
            try:
                zk.create(task_path(appname, eventnode))
            except kazoo.client.NodeExistsError:
                pass

        if event in ['aborted', 'killed', 'finished']:
            scheduled_node = scheduled_path(appname)
            logging.info(scheduled_node)
            logging.info('Unscheduling, event=%s: %s', event, scheduled_node)
            if zk.exists(scheduled_node):
                zk.delete(scheduled_node)
        os.unlink(path)

def join_zookeeper_path(root, *child):
    """"Returns zookeeper path joined by slash."""
    return '/'.join((root,) + child)


def make_path_f(zkpath):
    """"Return closure that will construct node path."""
    return staticmethod(functools.partial(join_zookeeper_path, zkpath))

def task_path(appname, eventnode):
    return '/'.join([TASKS]+appname.split('#'))+'/'+eventnode

def scheduled_path(appname):
    return SCHEDULED+'/'+appname

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppeventSvc)
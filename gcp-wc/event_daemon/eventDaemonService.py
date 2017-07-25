"""Event Daemon Service.

There is single event manager process per desktop.

Each server subscribes to the content of /servers/<desktopname> Zookeeper node.

The content contains the list of all apps currently scheduled to run on the
desktop.

Applications that are scheduled to run on the desktop are mirrored in the
'cache' directory.
"""
import os
import glob
import yaml
import kazoo
import docker
import tempfile
import socket
import collections
import functools
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join(os.path.join(os.getenv("workDirectory"),'log'), 'eventDaemonSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_SEEN_FILE = '.seen'
CACHE_DIR = 'cache'
RUNNING_DIR = 'running'

PLACEMENT = '/placement'
SERVER_PRESENCE = '/server.presence'
SCHEDULED = '/scheduled'

_HOSTNAME = socket.gethostname()

class EventDaemonSvc (win32serviceutil.ServiceFramework):
    """Event Daemon Service"""

    _svc_name_ = "EventDaemonService"
    _svc_display_name_ = "EventDaemonService"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self.root = os.getenv("workDirectory")
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        master_hosts = os.getenv("zookeeper")
        zk = KazooClient(hosts = master_hosts)
        zk.start()
        seen = zk.handler.event_object()
        # start not ready
        seen.clear()
        while True:
            # Wait for presence node to appear. Once up, syncronize the placement.
            @zk.DataWatch(path.server_presence(_HOSTNAME))
            def _server_presence_update(data, _stat, event):
                """Watch server presence"""
                if data is None and event is None:
                    # The node is not there yet, wait
                    logging.info('Server node missing.')
                    seen.clear()
                    cache_notify(self.root, False)
                elif event is not None and event.type == 'DELETED':
                    seen.set()
                    apps = zk.get_children(path.placement(_HOSTNAME))
                    synchronize(zk, apps, self.root)
                    logging.info('Presence node deleted.')
                    seen.clear()
                    cache_notify(self.root, False)
                else:
                    # logging.info('Presence is up.')
                    seen.set()
                    apps = zk.get_children(path.placement(_HOSTNAME))
                    synchronize(zk, apps, self.root)
                return True
            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break

def synchronize(zk, expected, root):
    """synchronize local app cache with the expected list.

    :param expected:
        List of instances expected to be running on the server.
    :type expected:
        ``list``
    :param zk:
        connection with zookeeper
    :type zk:
        "KazooClient"
    """
    expected_set = set(expected)
    current_set = {
        os.path.basename(manifest)
        for manifest in glob.glob(os.path.join(os.path.join(root, CACHE_DIR), '*'))
    }
    extra = current_set - expected_set
    missing = expected_set - current_set

    logging.info('expected : %s', ','.join(expected_set))
    logging.info('actual   : %s', ','.join(current_set))
    logging.info('extra    : %s', ','.join(extra))
    logging.info('missing  : %s', ','.join(missing))

    # If app is extra, remove the entry from the cache
    for app in extra:
        if os.path.exists(os.path.join(os.path.join(root, RUNNING_DIR), app)):
            with open(os.path.join(os.path.join(root, RUNNING_DIR), app)) as f:
                manifest_data = yaml.load(stream=f)
            try:
                client = docker.from_env()
                if client.containers.get(manifest_data['container_id']).status == 'running':
                    client.containers.get(manifest_data['container_id']).kill()
            except:
                pass
        manifest = os.path.join(os.path.join(root, CACHE_DIR), app)
        if os.path.exists(manifest):
            os.unlink(manifest)
        logging.info('Deleted cache manifest: %s', manifest)

    # If app is missing, fetch its manifest in the cache
    for app in missing:
        cache(zk, app, root)

def cache(zk, app, root):
    """Reads the manifest from Zk and stores it as YAML in <cache>/<app>.
    """
    appnode = path.scheduled(app)
    placement_node = path.placement(_HOSTNAME, app)
    manifest_file = None
    try:
        manifest = zkget(zk, appnode)
        # TODO: need a function to parse instance id from name.
        manifest['task'] = app[app.index('#') + 1:]

        placement_info = zkget(zk, placement_node)
        if placement_info is not None:
            manifest.update(placement_info)

        manifest_file = os.path.join(os.path.join(root, CACHE_DIR), app)
        with tempfile.NamedTemporaryFile(dir=os.path.join(root, CACHE_DIR),
                                          prefix='.%s-' % app,
                                          delete=False,
                                          mode='w') as temp_manifest:
            yaml.dump(manifest, stream=temp_manifest)
        os.rename(temp_manifest.name, manifest_file)
        logging.info('Created cache manifest: %s', manifest_file)

    except kazoo.exceptions.NoNodeError:
        logging.info('App %r not found', app)

def cache_notify(root, is_seen):
    """Sent a cache status notification event.

    Note: this needs to be an event, not a once time state change so
    that if appcfgmgr restarts after we enter the ready state, it will
    still get notified that we are ready.

    :params ``bool`` is_seen:
        True if the server is seen by the scheduler.
    """
    if is_seen:
        # Mark the cache folder as ready.
        if not os.path.exists(os.path.join(os.path.join(root, CACHE_DIR), _SEEN_FILE)):
            fp = open(os.path.join(os.path.join(root, CACHE_DIR), _SEEN_FILE), 'w')
            fp.close()
        with open(os.path.join(os.path.join(root, CACHE_DIR), _SEEN_FILE), 'w+'):
            pass
    else:
        # Mark the cache folder as outdated.
        if os.path.exists(os.path.join(os.path.join(root, CACHE_DIR), _SEEN_FILE)):
            os.unlink(os.path.join(os.path.join(root, CACHE_DIR), _SEEN_FILE))

def zkget(zkclient, path, watcher=None, strict=True, need_metadata=False):
    """Read content of Zookeeper node and return YAML parsed object."""
    data, metadata = zkclient.get(path, watch=watcher)

    result = None
    if data is not None:
        try:
            result = yaml.load(data)
        except yaml.YAMLError:
            if strict:
                raise
            else:
                result = data

    if need_metadata:
        return result, metadata
    else:
        return result

def join_zookeeper_path(root, *child):
    """"Returns zookeeper path joined by slash."""
    return '/'.join((root,) + child)


def make_path_f(zkpath):
    """"Return closure that will construct node path."""
    return staticmethod(functools.partial(join_zookeeper_path, zkpath))


path = collections.namedtuple('path', """
    placement
    server_presence
    scheduled
    """)

path.placement = make_path_f(PLACEMENT)
path.server_presence = make_path_f(SERVER_PRESENCE)
path.scheduled = make_path_f(SCHEDULED)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(EventDaemonSvc)
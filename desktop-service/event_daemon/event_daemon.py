"""Listens to desktop events.

There is single event manager process per desktop.

Each server subscribes to the content of /servers/<desktopname> Zookeeper node.

The content contains the list of all apps currently scheduled to run on the
desktop.

Applications that are scheduled to run on the desktop are mirrored in the
'cache' directory.
"""

import socket
import os
import yaml
import time
import logging.config
import glob
import tempfile

import kazoo
from kazoo.client import KazooClient

import sys
sys.path.append("..")
import zknamespace as z
import appenv
import fs
import zkutils

#logging
logging.basicConfig(filename = os.path.join("../../log", 'event_daemon.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_SEEN_FILE = '.seen'


class EventMgr(object):
    """Mirror Zookeeper scheduler event into node app cache events."""

    __slots__ = (
        'tm_env',
        '_hostname',
        'zk'
    )

    def __init__(self, root, zk):
        logging.info('init eventmgr: %s', root)
        self.tm_env = appenv.WindowsAppEnvironment(root)
        self._hostname = socket.gethostname()
        self.zk = zk;

    def run(self):
        """Establish connection to Zookeeper and subscribes to desktop events."""
        seen = self.zk.handler.event_object()
        #start not ready
        seen.clear()
        while True:
            # Wait for presence node to appear. Once up, syncronize the placement.
            @self.zk.DataWatch(z.path.server_presence(self._hostname))
            def _server_presence_update(data, _stat, event):
                """Watch server presence"""
                if data is None and event is None:
                    #The node is not there yet, wait
                    logging.info('Server node missing.')
                    seen.clear()
                    self._cache_notify(False)
                elif event is not None and event.type == 'DELETED':
                    logging.info('Presence node deleted.')
                    seen.clear()
                    self._cache_notify(False)
                else:
                    logging.info('Presence is up.')
                    seen.set()
                    apps = self.zk.get_children(z.path.placement(self._hostname))
                    self._synchronize(self.zk, apps)
                return True
            time.sleep(1)

    def _synchronize(self, zk, expected):
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
            for manifest in glob.glob(os.path.join(self.tm_env.cache_dir, '*'))
        }
        extra = current_set - expected_set
        missing = expected_set - current_set

        # logging.info('expected : %s', ','.join(expected_set))
        # logging.info('actual   : %s', ','.join(current_set))
        # logging.info('extra    : %s', ','.join(extra))
        # logging.info('missing  : %s', ','.join(missing))

        # If app is extra, remove the entry from the cache
        for app in extra:
            manifest = os.path.join(self.tm_env.cache_dir, app)
            os.unlink(manifest)
            logging.info('Deleted cache manifest: %s', manifest)

        # If app is missing, fetch its manifest in the cache
        for app in missing:
            self._cache(zk, app)

    def _cache(self, zk, app):
        """Reads the manifest from Zk and stores it as YAML in <cache>/<app>.
        """
        appnode = z.path.scheduled(app)
        placement_node = z.path.placement(self._hostname, app)
        manifest_file = None
        try:
            manifest = zkutils.get(zk, appnode)
            # TODO: need a function to parse instance id from name.
            manifest['task'] = app[app.index('#') + 1:]

            placement_info = zkutils.get(zk, placement_node)
            if placement_info is not None:
                manifest.update(placement_info)

            manifest_file = os.path.join(self.tm_env.cache_dir, app)
            with tempfile.NamedTemporaryFile(dir=self.tm_env.cache_dir,
                                             prefix='.%s-' % app,
                                             delete=False,
                                             mode='w') as temp_manifest:
                yaml.dump(manifest, stream=temp_manifest)
            os.rename(temp_manifest.name, manifest_file)
            logging.info('Created cache manifest: %s', manifest_file)

        except kazoo.exceptions.NoNodeError:
            logging.info('App %r not found', app)

    def _cache_notify(self, is_seen):
        """Sent a cache status notification event.

        Note: this needs to be an event, not a once time state change so
        that if appcfgmgr restarts after we enter the ready state, it will
        still get notified that we are ready.

        :params ``bool`` is_seen:
            True if the server is seen by the scheduler.
        """
        if is_seen:
            # Mark the cache folder as ready.
            with open(os.path.join(self.tm_env.cache_dir, _SEEN_FILE), 'w+'):
                pass
        else:
            # Mark the cache folder as outdated.
            fs.rm_safe(os.path.join(self.tm_env.cache_dir, _SEEN_FILE))

root = os.path.abspath('../..')
master_hosts = '192.168.1.119:2181'
zk = KazooClient(hosts = master_hosts)
zk.start()
eventDaemonService = EventMgr(root, zk)
eventDaemonService.run()

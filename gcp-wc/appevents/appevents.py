"""Process application events."""


import tempfile
import logging.config
import os
import time
import socket

import kazoo.client
from kazoo.client import KazooClient
import yaml

import sys
sys.path.append("..")

import exc
import appenv
import dirwatch
import zkutils
import zknamespace as z


#logging
logging.basicConfig(filename = os.path.join("../../log", 'appevents.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_SERVERS_ACL = zkutils.make_role_acl('servers', 'rwcd')

_HOSTNAME = socket.gethostname()


def post(events_dir, event):
    """Post application event to event directory.
    """
    logging.debug('post: %s: %r', events_dir, event)
    (
        _ts,
        _src,
        instanceid,
        event_type,
        event_data,
        payload
    ) = event.to_data()
    filename = '%s,%s,%s,%s' % (
        time.time(),
        instanceid,
        event_type,
        ('' if event_data is None else event_data)
    )
    with tempfile.NamedTemporaryFile(dir=events_dir,
                                     delete=False,
                                     prefix='.tmp', mode='w') as temp:
        if isinstance(payload, str):
            temp.write(payload)
        else:
            yaml.dump(payload, stream=temp)
    os.rename(temp.name, os.path.join(events_dir, filename))


class AppEventsWatcher(object):
    """Publish app events from the queue."""
    __slots__ = (
        'zk',
        'tm_env',
        'events_dir'
    )

    def __init__(self, zk, events_dir):
        self.tm_env = appenv.WindowsAppEnvironment(root = root)
        self.zk = zk
        self.events_dir = self.tm_env.app_events_dir

    def run(self):
        """Monitores events directory and publish events."""

        watch = dirwatch.DirWatcher(self.events_dir)
        watch.on_created = self._on_created

        for eventfile in os.listdir(self.events_dir):
            filename = os.path.join(self.events_dir, eventfile)
            self._on_created(filename)

        while True:
            if watch.wait_for_events(60):
                watch.process_events()

    @exc.exit_on_unhandled
    def _on_created(self, path):
        """This is the handler function when new files are seen"""
        if not os.path.exists(path):
            return

        localpath = os.path.basename(path)
        if localpath.startswith('.'):
            return

        logging.info('New event file - %r', path)

        eventtime, appname, event, data = localpath.split(',', 4)
        with open(path, mode='rb') as f:
            eventnode = '%s,%s,%s,%s' % (eventtime, _HOSTNAME, event, data)
            logging.debug('Creating %s', z.path.task(appname, eventnode))
            try:
                zkutils.with_retry(
                    self.zk.create,
                    z.path.task(appname, eventnode),
                    f.read(),
                    acl=[_SERVERS_ACL],
                    makepath=True
                )
            except kazoo.client.NodeExistsError:
                pass

        if event in ['aborted', 'killed', 'finished']:
            scheduled_node = z.path.scheduled(appname)
            logging.info('Unscheduling, event=%s: %s', event, scheduled_node)
            zkutils.with_retry(
                zkutils.ensure_deleted, self.zk,
                scheduled_node
            )

            # For terminal state, update the task node with exit summary.
            try:
                zkutils.with_retry(
                    zkutils.update,
                    self.zk,
                    z.path.task(appname),
                    {'state': event,
                     'when': eventtime,
                     'host': _HOSTNAME,
                     'data': data}
                )
            except kazoo.client.NoNodeError:
                logging.warn('Task node not found: %s', z.path.task(appname))

        os.unlink(path)

if __name__ == '__main__':
    #root = os.path.abspath('../..')
    root = 'C:/tmp'
    master_hosts = '192.168.1.119:2181'
    zk = KazooClient(hosts = master_hosts)
    zk.start()
    watcher = AppEventsWatcher(zk,root)
    watcher.run()

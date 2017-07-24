"""State Monitor Service.

Monitor the state of running containers.
"""
import os
import abc
import enum
import time
import yaml
import glob
import docker
import socket
import shutil
import tempfile
import functools
import collections
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join(os.path.join(os.getenv("workDirectory"),'log'), 'stateMonitorSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

SERVERS = '/servers'
SERVER_PRESENCE = '/server.presence'
SCHEDULED = '/scheduled'

_HOSTNAME = socket.gethostname()

CACHE_DIR = 'cache'
RUNNING_DIR = 'running'
APP_EVENTS_DIR = 'appevents'
CLEANUP_DIR = 'cleanup'

class StateMonitorSvc (win32serviceutil.ServiceFramework):
    """State Monitor Service"""

    _svc_name_ = "StateMonitorService"
    _svc_display_name_ = "StateMonitorService"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self.root = os.getenv("workDirectory")
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        """Monitor the state of running containers

        running containers: get ids from ../running
        running->exited

        if exited code equals 0, finished
        else, aborted and so on.
        if finished, docker rm container and state change to deleted.
        """
        master_hosts = os.getenv("zookeeper")
        zk = KazooClient(hosts = master_hosts)
        zk.start()
        client = docker.from_env()
        while True:
            running_containers = {}
            running_apps = {
                os.path.basename(manifest)
                for manifest in glob.glob(os.path.join(os.path.join(self.root, RUNNING_DIR), '*'))
            }
            cleanup_apps = {
                os.path.basename(manifest)
                for manifest in glob.glob(os.path.join(os.path.join(self.root, CLEANUP_DIR), '*'))
            }
            for app in running_apps-cleanup_apps:
                with open(os.path.join(os.path.join(self.root, RUNNING_DIR), app)) as f:
                    manifest_data = yaml.load(stream=f)
                running_containers[manifest_data['container_id']] = app

            exited_containers = set()
            for exited_container in client.containers.list(all, filters={"status": "exited"}):
                exited_containers.add(exited_container.id)

            finished_containers = set()
            for finished_container in client.containers.list(all, filters={"exited": "0"}):
                finished_containers.add(finished_container.id)

            killed_containers = set()
            for killed_container in client.containers.list(all, filters={"exited": "137"}):
                killed_containers.add(killed_container.id)

            aborted_containers = {}
            for exited_code in range(1, 256):
                if (exited_code != 137):
                    for container in client.containers.list(all, filters={"exited": str(exited_code)}):
                        aborted_containers[container.id] = exited_code

            for container_id in running_containers:
                if container_id in exited_containers:
                    instance_name = running_containers.get(container_id)
                    if (os.path.exists(os.path.join(os.path.join(self.root, RUNNING_DIR), instance_name))):
                        with open(os.path.join(os.path.join(self.root, RUNNING_DIR), instance_name)) as f:
                            manifest_data = yaml.load(stream=f)

                        # if container is normally finished
                        if container_id in finished_containers:
                            #create exited node
                            logging.info("exited: %s", running_containers.get(container_id))
                            post(
                                os.path.join(self.root, APP_EVENTS_DIR),
                                ServiceExitedTraceEvent(
                                    instanceid=instance_name,
                                    uniqueid=container_id,
                                    service=manifest_data['services'][0]['name'],
                                    rc='0',
                                    signal='0'
                                )
                            )
                            # create finished node
                            logging.info("finished: %s", running_containers.get(container_id))
                            post(
                                os.path.join(self.root, APP_EVENTS_DIR),
                                FinishedTraceEvent(
                                    instanceid=instance_name,
                                    rc='0',
                                    signal='0',
                                    payload=''
                                )
                            )
                            zk.delete(path.scheduled(instance_name))
                            if os.path.exists(os.path.join(os.path.join(self.root, RUNNING_DIR), running_containers.get(container_id))):
                                shutil.copy(os.path.join(os.path.join(self.root, RUNNING_DIR), running_containers.get(container_id)),
                                            os.path.join(self.root, CLEANUP_DIR))
                        # if container is killed
                        elif container_id in killed_containers:
                            # create exited node
                            logging.info("exited: %s", running_containers.get(container_id))
                            post(
                                os.path.join(self.root, APP_EVENTS_DIR),
                                ServiceExitedTraceEvent(
                                    instanceid=instance_name,
                                    uniqueid=container_id,
                                    service=manifest_data['services'][0]['name'],
                                    rc='137',
                                    signal='137'
                                )
                            )
                            # create killed node
                            logging.info("killed: %s", running_containers.get(container_id))
                            post(
                                os.path.join(self.root, APP_EVENTS_DIR),
                                KilledTraceEvent(
                                    instanceid=instance_name,
                                    # is_oom=bool(exitinfo.get('oom')),
                                    is_oom=False,
                                )
                            )
                            if os.path.exists(os.path.join(os.path.join(self.root, RUNNING_DIR), running_containers.get(container_id))):
                                shutil.copy(os.path.join(os.path.join(self.root, RUNNING_DIR), running_containers.get(container_id)),
                                            os.path.join(self.root, CLEANUP_DIR))
                        # if container is aborted
                        else:
                            # create exited node
                            logging.info("exited: %s", running_containers.get(container_id))
                            post(
                                os.path.join(self.root, APP_EVENTS_DIR),
                                ServiceExitedTraceEvent(
                                    instanceid=instance_name,
                                    uniqueid=container_id,
                                    service=manifest_data['services'][0]['name'],
                                    rc=str(aborted_containers.get(container_id)),
                                    signal=str(aborted_containers.get(container_id))
                                )
                            )
                            # create aborted node
                            logging.info("aborted: %s", running_containers.get(container_id))
                            post(
                                os.path.join(self.root, APP_EVENTS_DIR),
                                AbortedTraceEvent(
                                    why=str(aborted_containers.get(container_id)),
                                    instanceid=instance_name,
                                    payload=None
                                )
                            )
                            if os.path.exists(os.path.join(os.path.join(self.root, RUNNING_DIR), running_containers.get(container_id))):
                                shutil.copy(os.path.join(os.path.join(self.root, RUNNING_DIR), running_containers.get(container_id)),
                                            os.path.join(self.root, CLEANUP_DIR))

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
    server
    scheduled
    """)

path.scheduled = make_path_f(SCHEDULED)
path.server_presence = make_path_f(SERVER_PRESENCE)
path.server = make_path_f(SERVERS)

def post(events_dir, event):
    """Post application event to event directory.
    """
    logging.info('post: %s: %r', events_dir, event)
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

class AppTraceEvent(object, metaclass=abc.ABCMeta):
    """Parent class of all trace events.

    Contains the basic attributes of all events as well as the factory method
    `from_data` that instanciate an event object from its data representation.

    All event classes must derive from this class.
    """

    __slots__ = (
        'event_type',
        'timestamp',
        'source',
        'instanceid',
        'payload',
    )

    def __init__(self,
                 timestamp=None, source=None, instanceid=None, payload=None):
        self.event_type = AppTraceEventTypes(self.__class__).name
        if timestamp is None:
            self.timestamp = None
        else:
            self.timestamp = float(timestamp)
        self.source = source
        self.payload = payload
        self.instanceid = instanceid

    @abc.abstractproperty
    def event_data(self):
        """Abstract property that returns the an event's event_data.
        """
        pass

    @classmethod
    def _class_from_type(cls, event_type):
        """Return the class for a given event_type.
        """
        etype = getattr(AppTraceEventTypes, event_type, None)
        if etype is None:
            logging.infowarning('Unknown event type %r', event_type)
            return
        eclass = etype.value
        return eclass

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        """Intantiate an event from given event data.
        """
        eclass = cls._class_from_type(event_type)
        if eclass is None:
            return None

        try:
            event = eclass.from_data(
                timestamp=timestamp,
                source=source,
                instanceid=instanceid,
                event_type=event_type,
                event_data=event_data,
                payload=payload
            )
        except Exception:
            logging.info('Failed to parse event type %r:', event_type,
                            exc_info=True)
            event = None

        return event

    def to_data(self):
        """Returns a 6 tuple represtation of an event.
        """
        return (
            self.timestamp,
            self.source,
            self.instanceid,
            self.event_type,
            self.event_data,
            self.payload
        )

    @classmethod
    def from_dict(cls, event_data):
        """Instantiate an event from a dict of its data.
        """
        event_type = event_data.pop('event_type')
        eclass = cls._class_from_type(event_type)
        if eclass is None:
            return None

        try:
            event = eclass(**event_data)

        except Exception:
            logging.info('Failed to instanciate event type %r:', event_type,
                            exc_info=True)
            event = None

        return event

    def to_dict(self):
        """Returns a dictionary representation of an event.
        """
        return {
            k: getattr(self, k)
            for k in super(self.__class__, self).__slots__ + self.__slots__
        }

    def __eq__(self, other):
        return (
            issubclass(type(other), AppTraceEvent) and
            self.to_dict() == other.to_dict()
        )

    def __repr__(self):
        return '{classname}<{data}>'.format(
            classname=self.__class__.__name__[:-len('TraceEvent')],
            data={k: getattr(self, k)
                  for k in self.__slots__}
        )


class ScheduledTraceEvent(AppTraceEvent):
    """Event emitted when a container instance is placed on a node.
    """

    __slots__ = (
        'where',
    )

    def __init__(self, where,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(ScheduledTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.where = where

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            where=event_data
        )

    @property
    def event_data(self):
        return self.where


class PendingTraceEvent(AppTraceEvent):
    """Event emitted when a container instance is seen by the scheduler but not
    placed on a node.
    """

    __slots__ = (
    )

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )

    @property
    def event_data(self):
        return None


class ConfiguredTraceEvent(AppTraceEvent):
    """Event emitted when a container instance is configured on a node.
    """

    __slots__ = (
        'uniqueid',
    )

    def __init__(self, uniqueid,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(ConfiguredTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.uniqueid = uniqueid

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            uniqueid=event_data
        )

    @property
    def event_data(self):
        return self.uniqueid


class DeletedTraceEvent(AppTraceEvent):
    """Event emitted when a container instance is deleted from the scheduler.
    """

    __slots__ = (
    )

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )

    @property
    def event_data(self):
        return None


class FinishedTraceEvent(AppTraceEvent):
    """Event emitted when a container instance finished.
    """

    __slots__ = (
        'rc',
        'signal',
    )

    def __init__(self, rc, signal,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(FinishedTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.rc = int(rc)
        self.signal = int(signal)

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        rc, signal = event_data.split('.', 2)
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            rc=rc,
            signal=signal
        )

    @property
    def event_data(self):
        return '{rc}.{signal}'.format(
            rc=self.rc,
            signal=self.signal
        )


class AbortedTraceEvent(AppTraceEvent):
    """Event emitted when a container instance was aborted.
    """

    __slots__ = (
        'why',
    )

    def __init__(self, why,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(AbortedTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.why = why

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            why=event_data
        )

    @property
    def event_data(self):
        return self.why


class KilledTraceEvent(AppTraceEvent):
    """Event emitted when a container instance was killed.
    """

    __slots__ = (
        'is_oom',
    )

    def __init__(self, is_oom,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(KilledTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.is_oom = is_oom

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            is_oom=(event_data == 'oom')
        )

    @property
    def event_data(self):
        return '{oom}'.format(
            oom=('oom' if self.is_oom else '')
        )


class ServiceRunningTraceEvent(AppTraceEvent):
    """Event emitted when a service of container instance started.
    """

    __slots__ = (
        'uniqueid',
        'service',
    )

    def __init__(self, uniqueid, service,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(ServiceRunningTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.uniqueid = uniqueid
        self.service = service

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value
        parts = event_data.split('.')
        uniqueid = parts.pop(0)
        service = '.'.join(parts)
        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            uniqueid=uniqueid,
            service=service
        )

    @property
    def event_data(self):
        return '{uniqueid}.{service}'.format(
            uniqueid=self.uniqueid,
            service=self.service
        )


class ServiceExitedTraceEvent(AppTraceEvent):
    """Event emitted when a service of container instance exited.
    """

    __slots__ = (
        'uniqueid',
        'service',
        'rc',
        'signal',
    )

    def __init__(self, uniqueid, service, rc, signal,
                 timestamp=None, source=None, instanceid=None, payload=None):
        super(ServiceExitedTraceEvent, self).__init__(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload
        )
        self.uniqueid = uniqueid
        self.service = service
        self.rc = int(rc)
        self.signal = int(signal)

    @classmethod
    def from_data(cls, timestamp, source, instanceid, event_type, event_data,
                  payload=None):
        assert cls == getattr(AppTraceEventTypes, event_type).value

        parts = event_data.split('.')
        uniqueid = parts.pop(0)
        signal = parts.pop()
        rc = parts.pop()
        service = '.'.join(parts)

        return cls(
            timestamp=timestamp,
            source=source,
            instanceid=instanceid,
            payload=payload,
            uniqueid=uniqueid,
            service=service,
            rc=rc,
            signal=signal
        )

    @property
    def event_data(self):
        return '{uniqueid}.{service}.{rc}.{signal}'.format(
            uniqueid=self.uniqueid,
            service=self.service,
            rc=self.rc,
            signal=self.signal
        )


class AppTraceEventTypes(enum.Enum):
    """Enumeration of all event type names.
    """
    aborted = AbortedTraceEvent
    configured = ConfiguredTraceEvent
    deleted = DeletedTraceEvent
    finished = FinishedTraceEvent
    killed = KilledTraceEvent
    pending = PendingTraceEvent
    scheduled = ScheduledTraceEvent
    service_exited = ServiceExitedTraceEvent
    service_running = ServiceRunningTraceEvent

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(StateMonitorSvc)
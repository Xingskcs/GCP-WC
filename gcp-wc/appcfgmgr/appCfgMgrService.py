"""AppCfgMgr Service.

Applications that are scheduled to run on the server are mirrored in the
cache directory

Configure and running the apps.
"""
import os
import abc
import glob
import time
import yaml
import docker
import socket
import tempfile
import functools
import collections
import logging.config
from kazoo.client import KazooClient

import enum

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join("C:/tmp/log", 'appCfgMgrSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

_HOSTNAME = socket.gethostname()

CACHE_DIR = 'cache'
RUNNING_DIR = 'running'
APP_EVENTS_DIR = 'appevents'

RUNNING = '/running'
SCHEDULED = '/scheduled'

class AppCfgMgrSvc (win32serviceutil.ServiceFramework):
    """AppCfgMgr Service"""

    _svc_name_ = "AppCfgMgrService"
    _svc_display_name_ = "AppCfgMgrService"

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
        zk = KazooClient(hosts=master_hosts)
        zk.start()
        while True:
            cached_files = glob.glob(
                os.path.join(os.path.join(self.root, CACHE_DIR), '*')
            )
            running_links = glob.glob(
                os.path.join(os.path.join(self.root, RUNNING_DIR), '*')
            )
            for file_name in set(cached_files) - set(running_links):
                if not os.path.exists(os.path.join(os.path.join(self.root, RUNNING_DIR), os.path.basename(file_name))):
                    configure(zk, os.path.basename(file_name))
            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break

def configure(zk, instance_name):
    """Configures and starts the instance based on instance cached event.

    :param ``str`` instance_name:
        Name of the instance to configure
    :returns ``bool``:
        True for successfully configured container.
    """
    if instance_name[0] == '.':
        # Ignore all dot files
        return
    time.sleep(3)
    event_file = os.path.join(
        os.path.join('C:/tmp', CACHE_DIR),
        instance_name
    )

    logging.info("configuring %s", instance_name)
    with open(event_file) as f:
        manifest_data = yaml.load(stream=f)
    client = docker.from_env()
    docker_container = client.containers.create(image = 'resource',
                                                command = manifest_data['services'][0]['command'])

    if docker_container in client.containers.list(all):
        post(
            os.path.join('C:/tmp', APP_EVENTS_DIR),
            ConfiguredTraceEvent(
                instanceid=instance_name,
                uniqueid=docker_container.id
            )
        )
        logging.info("configure success %s", instance_name)


    logging.info("starting %s", instance_name)
    canstarted = True
    try:
        docker_container.start()
    except docker.errors.APIError:
        canstarted = False
    if canstarted:
        manifest_file = os.path.join(os.path.join('C:/tmp', RUNNING_DIR), instance_name)
        manifest = {'container_id':docker_container.id}
        if not os.path.exists(manifest_file):
            with tempfile.NamedTemporaryFile(dir=os.path.join('C:/tmp', RUNNING_DIR),
                                            prefix='.%s-' % instance_name,
                                            delete=False,
                                            mode='w') as temp_manifest:
                yaml.dump(manifest, stream=temp_manifest)
            os.rename(temp_manifest.name, manifest_file)
        logging.info('Created running manifest: %s', manifest_file)

        post(
            os.path.join('C:/tmp', APP_EVENTS_DIR),
            ServiceRunningTraceEvent(
                instanceid=instance_name,
                uniqueid=docker_container.id,
                service=manifest_data['services'][0]['name']
            )
        )
        # app_data = zk.get(path_scheduled(instance_name))
        # zk.create(path_running(instance_name), app_data.encode('utf-8'))
        logging.info("running %s", instance_name)

def path_scheduled(instance_name):
    return SCHEDULED+'/'+instance_name

def path_running(instance_name):
    return RUNNING +'/'+instance_name

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
    win32serviceutil.HandleCommandLine(AppCfgMgrSvc)
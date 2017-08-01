"""Watchdog Service.

Watch the state of service and the state of connection with zookeeper.
"""
import os
import time
import socket
import datetime
import logging.config
from kazoo.client import KazooClient

import win32serviceutil
import win32service
import win32event

#logging
logging.basicConfig(filename = os.path.join(os.path.join(os.getenv("workDirectory"),'log'), 'watchdogSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class ServiceManager(object):
    """Manage Windows service"""

    def __init__(self, name):
        """
        name: service name
        """
        self.name = name

        self.wait_time = 0.5
        self.delay_time = 10
        self.scm = win32service.OpenSCManager(None, None, win32service.SC_MANAGER_ALL_ACCESS)

        if self.is_exists():
            try:
                self.handle = win32service.OpenService(self.scm, self.name, win32service.SC_MANAGER_ALL_ACCESS)
            except Exception as e:
                logging.info(e)
        else:
            logging.info('service %s is not installed.', self.name)

    def is_stop(self):
        """Check service whether stop"""
        flag = False
        try:
            if self.handle:
                ret = win32service.QueryServiceStatus(self.handle)
                flag = ret[1] != win32service.SERVICE_RUNNING
        except Exception as e:
            logging.info(e)
        return flag

    def start(self):
        """Start service"""
        try:
            if self.handle:
                win32service.StartService(self.handle, None)
        except Exception as e:
            logging.info(e)
        status_info = win32service.QueryServiceStatus(self.handle)

        if status_info[1] == win32service.SERVICE_RUNNING:
            logging.info('Start %s successfully.', self.name)
            return 'Start %s successfully.'% (self.name)
        elif status_info[1] == win32service.SERVICE_START_PENDING:
            start_time = datetime.datetime.now()
            while True:
                if (datetime.datetime.now() - start_time).seconds > self.delay_time:
                    logging.info('Start %s too much time.', self.name)
                    return 'Start %s too much time.' % (self.name)

                time.sleep(self.wait_time)
                if win32service.QueryServiceStatus(self.handle)[1] == win32service.SERVICE_RUNNING:
                    logging.info('Start %s successfully.', self.name)
                    return 'Start %s successfully.' % (self.name)
        else:
            logging.info('Start %s fail.', self.name)
            return 'Start %s fail.' % (self.name)

    def stop(self):
        """Stop service"""
        try:
            status_info = win32service.ControlService(self.handle, win32service.SERVICE_CONTROL_STOP)
        except Exception as e:
            logging.info(e)
        if status_info[1] == win32service.SERVICE_STOPPED:
            logging.info('Stop %s successfully.', self.name)
            return 'Stop %s successfully.'% (self.name)
        elif status_info[1] == win32service.SERVICE_STOP_PENDING:
            start_time = datetime.datetime.now()
            while True:
                if (datetime.datetime.now() - start_time).seconds > self.delay_time:
                    logging.info('Stop %s too much time.', self.name)
                    return 'Stop %s too much time.' % (self.name)

                time.sleep(self.wait_time)
                if win32service.QueryServiceStatus(self.handle)[1] == win32service.SERVICE_STOPPED:
                    logging.info('Stop %s successfully.', self.name)
                    return 'Stop %s successfully.' % (self.name)
        else:
            logging.info('Stop %s fail.', self.name)
            return 'Stop %s fail.' % (self.name)

    def restart(self):
        """Restart service"""
        if not self.is_stop():
            self.stop()
        self.start()
        return win32service.QueryServiceStatus(self.handle)

    def status(self):
        """Get the state of service"""
        try:
            status_info = win32service.QueryServiceStatus(self.handle)
            status = status_info[1]
            if status == win32service.SERVICE_STOPPED:
                return "STOPPED"
            elif status == win32service.SERVICE_START_PENDING:
                return "STARTING"
            elif status == win32service.SERVICE_STOP_PENDING:
                return "STOPPING"
            elif status == win32service.SERVICE_RUNNING:
                return "RUNNING"
        except Exception as e:
            logging.info(e)

    def close(self):
        """Free resources"""
        try:
            if self.scm:
                win32service.CloseServiceHandle(self.handle)
                win32service.CloseServiceHandle(self.scm)
        except Exception as e:
            logging.info(e)

    def is_exists(self):
        """Check service whether installed"""
        statuses = win32service.EnumServicesStatus(self.scm, win32service.SERVICE_WIN32, win32service.SERVICE_STATE_ALL)
        for (short_name, desc, status) in statuses:
            if short_name == self.name:
                return True
        return False

SERVER_PRESENCE = '/server.presence'

_HOSTNAME = socket.gethostname()

screen_state_file = 'screen_state.txt'
_APPCFGMGR = 'AppCfgMgrService'
_APPEVENTS = 'AppeventService'
_CLEANUP = 'CleanupService'
_EVENTDAEMON = 'EventDaemonService'
_SCREENMONITOR = 'ScreenMonitorService'
_REGISTERZOOKEEPER = 'RegisterZookeeperService'
_STATEMONITOR = 'StateMonitorService'
_UPDATERESOURCES = 'UpdateResourcesService'

class WatchdogSvc (win32serviceutil.ServiceFramework):
    """Register Zookeeper Service"""

    _svc_name_ = "WatchdogService"
    _svc_display_name_ = "WatchdogService"

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
        zk = KazooClient(hosts=master_hosts)
        zk.start()

        services = []
        app_cfg = ServiceManager(_APPCFGMGR)
        services.append(app_cfg)
        app_events = ServiceManager(_APPEVENTS)
        services.append(app_events)
        clean_up = ServiceManager(_CLEANUP)
        services.append(clean_up)
        event_daemon = ServiceManager(_EVENTDAEMON)
        services.append(event_daemon)
        register_zk = ServiceManager(_REGISTERZOOKEEPER)
        services.append(register_zk)
        state_monitor = ServiceManager(_STATEMONITOR)
        services.append(state_monitor)
        update_resources = ServiceManager(_UPDATERESOURCES)
        services.append(update_resources)
        screen_monitor = ServiceManager(_SCREENMONITOR)
        services.append(screen_monitor)

        self._start(services)
        previous_state = zk.state
        while True:
            f = open(os.path.join(self.root, screen_state_file), 'r')
            screen_state = f.read()
            if screen_state == 'Lock':
                try:
                    if zk.state == 'CONNECTED':
                        self._start(services)
                        if self._serviceStatus(services):
                            # zk.ensure_path(server_presence())
                            pass
                        else:
                            self._stop(services)
                            if zk.exists(server_presence()):
                                zk.delete(server_presence())
                    else:
                        self._stop(services)
                        if zk.exists(server_presence()):
                            zk.delete(server_presence())
                        try:
                            zk.start()
                        except:
                            pass
                except:
                    pass
            else:
                time.sleep(5)
                self._stop(services)
                if zk.exists(server_presence()):
                    zk.delete(server_presence())
                try:
                    zk.start()
                except:
                    pass

            previous_state = zk.state
            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break
    # def SvcDoRun(self):
    #     master_hosts = os.getenv("zookeeper")
    #     zk = KazooClient(hosts=master_hosts)
    #     zk.start()
    #
    #     services = []
    #     app_cfg = ServiceManager(_APPCFGMGR)
    #     services.append(app_cfg)
    #     app_events = ServiceManager(_APPEVENTS)
    #     services.append(app_events)
    #     clean_up = ServiceManager(_CLEANUP)
    #     services.append(clean_up)
    #     event_daemon = ServiceManager(_EVENTDAEMON)
    #     services.append(event_daemon)
    #     register_zk = ServiceManager(_REGISTERZOOKEEPER)
    #     services.append(register_zk)
    #     state_monitor = ServiceManager(_STATEMONITOR)
    #     services.append(state_monitor)
    #     update_resources = ServiceManager(_UPDATERESOURCES)
    #     services.append(update_resources)
    #     screen_monitor = ServiceManager(_SCREENMONITOR)
    #     services.append(screen_monitor)
    #
    #     self._start(services)
    #     previous_state = zk.state
    #     while True:
    #         try:
    #             if zk.state == 'CONNECTED':
    #                 if previous_state == 'SUSPENDED':
    #                     self._start(services)
    #                 if self._serviceStatus(services):
    #                     #zk.ensure_path(server_presence())
    #                     pass
    #                 else:
    #                     self._stop(services)
    #                     if zk.exists(server_presence()):
    #                         zk.delete(server_presence())
    #             else:
    #                 self._stop(services)
    #                 if zk.exists(server_presence()):
    #                     zk.delete(server_presence())
    #                 try:
    #                     zk.start()
    #                 except:
    #                     pass
    #         except:
    #             pass
    #         previous_state = zk.state
    #         if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
    #             break


    def _start(self, services):
        for service in services:
            if service.status() == 'STOPPED':
                service.start()

    def _stop(self, services):
        for service in services:
            if service.status() == 'RUNNING':
                if service.name == 'ScreenMonitorService':
                    #os.system('TASKKILL /F /FI "services eq ScreenMonitorService"')
                    pass
                else:
                    service.stop()

    def _serviceStatus(self, services):
        for service in services:
            if service.status() == 'STOPPED':
                logging.info('failed service: '+str(service.name))
                return False
        return True

def server_presence():
    return SERVER_PRESENCE+'/'+_HOSTNAME



if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(WatchdogSvc)
    #app = ServiceManager('AppeventService')
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import time
from kazoo.client import KazooClient


class AppServerSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "TestService"
    _svc_display_name_ = "Test Service"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        master_hosts = '192.168.1.119:2181'
        zk = KazooClient(hosts = master_hosts)
        zk.start()
        while True:
            f = open('C:/tmp/screen_state.txt', 'r')
            screen_state = f.read()
            node_data = zk.get('/servers/node')
            # For desktop, we add a 'windows' label, in order to schedule better later.
            desktop_data = node_data[0].decode().replace('~', 'windows', 1)
            if not zk.exists('/servers/desktop1'):
                zk.create('/servers/desktop1', desktop_data.encode('utf-8'))
            if not zk.exists('/server.presence/desktop1'):
                zk.create('/server.presence/desktop1', desktop_data.encode('utf-8'))
            time.sleep(1)


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppServerSvc)
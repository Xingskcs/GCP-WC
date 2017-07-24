"""MonitorScreen Service.

Monitor the screen state, Lock or Unlock.
"""
from __future__ import print_function

import os
import sys
import socket
import logging.config

import win32serviceutil
import win32service
import win32event

try:
    import win32api as api
    import win32con as con
    import win32gui as gui
    import win32ts as ts
except ImportError:
    print("wtsmonitor: PyWin32 modules not found", file=sys.stderr)
    sys.exit(1)

try:
    import events
except ImportError:
    print("wtsmonitor: events.py not found", file=sys.stderr)
    sys.exit(1)

#logging
logging.basicConfig(filename = os.path.join(os.path.join(os.getenv("workDirectory"),'log'), 'screenMonitorSVC.txt'), filemode="w", level=logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('# %(asctime)s - %(name)s:%(lineno)d %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

#windows message
WM_WTSSESSION_CHANGE		= 0x2B1

# WM_WTSSESSION_CHANGE events (wparam)
WTS_CONSOLE_CONNECT		= 0x1
WTS_CONSOLE_DISCONNECT		= 0x2
WTS_REMOTE_CONNECT		= 0x3
WTS_REMOTE_DISCONNECT		= 0x4
WTS_SESSION_LOGON		= 0x5
WTS_SESSION_LOGOFF		= 0x6
WTS_SESSION_LOCK		= 0x7
WTS_SESSION_UNLOCK		= 0x8
WTS_SESSION_REMOTE_CONTROL	= 0x9

methods = {
	WTS_CONSOLE_CONNECT:		"ConsoleConnect",
	WTS_CONSOLE_DISCONNECT:		"ConsoleDisconnect",
	WTS_REMOTE_CONNECT:		"RemoteConnect",
	WTS_REMOTE_DISCONNECT:		"RemoteDisconnect",
	WTS_SESSION_LOGON:		"SessionLogon",
	WTS_SESSION_LOGOFF:		"SessionLogoff",
	WTS_SESSION_LOCK:		"SessionLock",
	WTS_SESSION_UNLOCK:		"SessionUnlock",
	WTS_SESSION_REMOTE_CONTROL:	"SessionRemoteControl",
}

screen_state_file = 'screen_state.txt'


class MonitorScreenSvc (win32serviceutil.ServiceFramework):
    """Screen Monitor Service"""

    _svc_name_ = "ScreenMonitorService"
    _svc_display_name_ = "ScreenMonitorService"

    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        self.root = os.getenv("workDirectory")
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        m = WTSMonitor(self.root, all_sessions=True)
        while True:
            m.start()
            if win32event.WaitForSingleObject(self.hWaitStop, 2000) == win32event.WAIT_OBJECT_0:
                break
            # m.stop()

class WTSMonitor():
    """Monitor Windows screen"""
    className = "WTSMonitor"
    wndName = "WTS Event Monitor"

    def __init__(self, root, all_sessions=False):
        wc = gui.WNDCLASS()
        wc.hInstance = hInst = api.GetModuleHandle(None)
        wc.lpszClassName = self.className
        wc.lpfnWndProc = self.WndProc
        self.classAtom = gui.RegisterClass(wc)
        self.root = root

        style = 0
        self.hWnd = gui.CreateWindow(self.classAtom, self.wndName,style, 0, 0, con.CW_USEDEFAULT, con.CW_USEDEFAULT,0, 0, hInst, None)
        gui.UpdateWindow(self.hWnd)

        if all_sessions:
            scope = ts.NOTIFY_FOR_ALL_SESSIONS
        else:
            scope = ts.NOTIFY_FOR_THIS_SESSION
        ts.WTSRegisterSessionNotification(self.hWnd, scope)

    def start(self):
        gui.PumpMessages()

    def stop(self):
        gui.PostMessage(0)

    def WndProc(self, hWnd, message, wParam, lParam):
        if message == WM_WTSSESSION_CHANGE:
            self.OnSession(wParam,lParam)
        elif message == con.WM_CLOSE:
            gui.DestroyWindow(hWnd)
        elif message == con.WM_DESTROY:
            gui.PostQuitMessage(0)
        elif message == con.WM_QUERYENDSESSION:
            return True

    def OnSession(self, event, sessionId):
        name = methods.get(event, "unknown")
        if name == 'SessionLock':
            logging.info("Scrren is locked!")
            f = open(os.path.join(self.root, screen_state_file),'w')
            f.write("Lock")
            f.close()
        elif name == 'SessionUnlock':
            logging.info("Screen is unlocked!")
            f = open(os.path.join(self.root, screen_state_file), 'w')
            f.write("Unlock")
            f.close()

        logging.info("event %s on session %d" % (methods.get(event, "unknown(0x%x)" % event), sessionId))
        try:
            method = getattr(events, name)
        except AttributeError:
            method = getattr(events, "default", lambda e, s: None)

        method(event, sessionId)


if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(MonitorScreenSvc)
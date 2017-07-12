#!python
from __future__ import print_function
import os
import sys
import yaml
import tempfile
import subprocess

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


class WTSMonitor():

    className = "WTSMonitor"
    wndName = "WTS Event Monitor"

    def __init__(self, all_sessions=False):
        wc = gui.WNDCLASS()
        wc.hInstance = hInst = api.GetModuleHandle(None)
        wc.lpszClassName = self.className
        wc.lpfnWndProc = self.WndProc
        self.classAtom = gui.RegisterClass(wc)
        #self.screenState = ScreenState()

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
            f = open('screen_state.txt','w')
            f.write("Lock")
            f.close()
        elif name == 'SessionUnlock':
            f = open('screen_state.txt', 'w')
            f.write("Unlock")
            f.close()

        print("event %s on session %d" % (methods.get(event, "unknown(0x%x)" % event), sessionId))

        try:
            method = getattr(events, name)
        except AttributeError:
            method = getattr(events, "default", lambda e, s: None)

        method(event, sessionId)

if __name__ == '__main__':
    m = WTSMonitor(all_sessions=True)
    m.start()
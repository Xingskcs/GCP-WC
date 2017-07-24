"""
Set environment, e.g. "JAVA_HOME".

Usage:
    env_set [name] [value] ["user" or "system"]
    "user" to add user path, default;
    "system" to add system path.
"""
import sys, os
import json
from winreg import *


def set_env(name, value, scope='user'):
    assert scope in ('user', 'system')
    if scope == 'user':
        key = HKEY_CURRENT_USER
        subkey = 'Environment'
    else:
        key = HKEY_LOCAL_MACHINE
        subkey = r'SYSTEM\CurrentControlSet\Control\Session Manager\Environment'

    key_handle = OpenKey(key, subkey, 0, KEY_ALL_ACCESS)
    SetValueEx(key_handle, name, 0, REG_EXPAND_SZ, value)
    key_handle.Close()


if __name__ == '__main__':
    with open('configure.json', 'r') as f:
        configure_data = json.load(f)
    for name in configure_data.keys():
        set_env(name, configure_data[name],'system')
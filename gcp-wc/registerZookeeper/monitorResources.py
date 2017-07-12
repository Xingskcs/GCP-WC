"""Monitor windows desktop cpu and memory usage.

This script is designed to monitor windows cpu and memory usage over a period of
time.User can define the start time,end time and frequency of monitor.This script
also provides the interface of monitor windows cpu and memory usage based on psutil
module.

An example of calling the interface is:
get_cpu_state(interval).where frequency = 1.0/interval
get_memory_state()
monitor_cpu_and_memory(start_time, end_time, interval)

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import time
import csv

import psutil

def get_cpu_state(interval=1.0):
    """Get windows desktop cpu state.

    Args:
        interval:float.The interval(s) between monitor. interval = 1.0 equals 1HZ

    Returns:
        windows desktop cpu state at a time
    """
    return psutil.cpu_percent(interval)

def get_memory_state(interval=1.0):
    """Get windows desktop memory state.

    Args:
        interval:float.The interval(s) between monitor. interval = 1.0 equals 1HZ

    Returns:
        windows desktop memory state at a time
    """
    phymem = psutil.virtual_memory()
    return phymem.percent

def get_interval_before_start_time(start_time, end_time, interval):
    """Computer the interval before start_time and monitor times,
       transfet start_time, end_time to timestamp.

    Args:
        start_time:The datetime timestamp.
                   start_time = datetime.datetime(year,month,day,hour,minute,second)
        end_time:The datetime timestamp.end_time =
                 datetime.datetime(year,month,day,hour,minute,second)
        interval:float.The interval(s) between monitor.
                 interval = 1.0 equals 1HZ

    Returns:
        interval_before_start_time:float.The interval before start_time.
        times:int.The monitor times.
    """
    #transfer start_time end_time to timestamp
    start_time = start_time.timestamp()
    end_time = end_time.timestamp()
    now = datetime.now().timestamp()
    #computer the interval_before_start_time
    if start_time < now:#if now > start_time
        interval_before_start_time = 0
    else:
        interval_before_start_time = start_time - now
    #computer the times
    if start_time >= now:
        monitor_interval = end_time - start_time
    else:
        monitor_interval = end_time - now
    if monitor_interval < 0:
        raise Exception("The input of start_time and end_time is illegal!")
    else:
        times = (monitor_interval)/interval
    return interval_before_start_time, int(times)

def monitorResources(interval=1.0):
    """Monitor windows desktop's resources useage


    Args:
        interval:float.The interval(s) between monitor.interval = 1.0 equals 1HZ

    Returns:
        False:monitor throw an exception or cause an error at any time.
        remain_cpu
        remain_mem
        remain_disk
    """
    remain_cpu = int(100-psutil.cpu_percent(interval))
    remain_mem = int(psutil.virtual_memory().available/1024/1024)
    remain_disk = int(psutil.disk_usage('/').free/1024/1024)
    return remain_cpu, remain_mem, remain_disk
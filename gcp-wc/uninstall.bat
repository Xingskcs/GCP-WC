@call :output>uninstall.log
exit
:output
@echo off

python .\watchdog\watchdogService.py stop
python .\appcfgmgr\appcfgMgrService.py stop
python .\appevents\appeventService.py stop
python .\cleanup\cleanupService.py stop
python .\event_daemon\eventDaemonService.py stop
python .\registerZookeeper\registerZookeeperService.py stop
python .\stateMonitor\stateMonitorService.py stop
python .\updateResources\updateResourcesService.py stop
TASKKILL /F /FI "services eq ScreenMonitorService"

python .\watchdog\watchdogService.py remove
python .\appcfgmgr\appcfgMgrService.py remove
python .\appevents\appeventService.py remove
python .\cleanup\cleanupService.py remove
python .\event_daemon\eventDaemonService.py remove
python .\monitorScreen\monitorScreenService.py remove
python .\registerZookeeper\registerZookeeperService.py remove
python .\stateMonitor\stateMonitorService.py remove
python .\updateResources\updateResourcesService.py remove

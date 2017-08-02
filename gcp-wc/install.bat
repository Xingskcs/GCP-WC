rem install script
@echo off

git pull origin master

pip install -r requirements.txt
python .\appcfgmgr\appcfgMgrService.py install
python .\appevents\appeventService.py install
python .\cleanup\cleanupService.py install
python .\event_daemon\eventDaemonService.py install
python .\monitorScreen\monitorScreenService.py install
python .\registerZookeeper\registerZookeeperService.py install
python .\stateMonitor\stateMonitorService.py install
python .\updateResources\updateResourcesService.py install
python .\watchdog\watchdogService.py install

python .\watchdog\watchdogService.py start

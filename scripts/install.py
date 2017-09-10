"""install script.

1) create work directory.
2)install windows service

"""
import os
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('install'+time.strftime("%a, %d %b %Y %H:%M:%S+0000", time.gmtime(time.time())).replace('  ', ' ')
                                  .replace(':',' ') + '.log', mode='w', encoding='UTF-8')
fileHandler.setLevel(logging.NOTSET)
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                              time.strftime("%a, %d %b %Y %H:%M:%S+0000", time.gmtime(time.time())))
consoleHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)
logger.addHandler(fileHandler)

#install_version
install_version="0.1"

def createWorkDirectory():
    """Create work directory.
    """
    root = os.getenv("workDirectory")
    dirs = ['appevents', 'cache', 'cleanup', 'log', 'running']
    files = ['screen_state.txt', 'installed_version.txt']
    for dir in dirs:
        if not os.path.exists(os.path.join(root, dir)):
            os.makedirs(os.path.join(root, dir))
    for file in files:
        f = open(os.path.join(root, file), 'w')
        f.close()
    log("Successfully created work directory")

    #write install version
    f = open(os.path.join(root, 'installed_version.txt'), 'w')
    f.write(install_version)
    f.close()

def log(information):
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                                  time.strftime("%a, %d %b %Y %H:%M:%S+0000", time.gmtime(time.time())))
    consoleHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    logging.info(information)

def main():
    createWorkDirectory()

    #change directory
    current_path = os.getcwd()
    os.chdir(current_path[:len(current_path)-(len('scripts')+1)])
    os.system('pip install -e .')#install gcp-wc
    log("Successfully installed gcp-wc")

    #install windows services
    services = ['app_config_manager_service', 'app_event_service', 'cleanup_service', 'event_daemon_service', 'monitor_screen_service'
                , 'register_zookeeper_service', 'state_monitor_service', 'update_resource_service', 'watchdog_service']
    service_names = ['AppCfgMgrService', 'AppeventService', 'CleanupService', 'EventDaemonService', 'ScreenMonitorService',
                     'RegisterZookeeperService', 'StateMonitorService', 'UpdateResourcesService', 'WatchdogService']
    dictionary = dict(zip(services, service_names))
    for service in services:
        os.system('python -m gcp_wc.{service_name} install'.format(service_name=service))
        log("Successfully installed {service_name}".format(service_name=dictionary[service]))
    os.system('python -m gcp_wc.watchdog_service start')
    log("Successfully starting WatchdogService")

if __name__ == '__main__':
    main()
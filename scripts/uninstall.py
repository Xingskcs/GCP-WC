"""uninstall script.

1)uninstall windows service.
2)delete work directory.

"""
import os
import time
import shutil
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('uninstall'+time.strftime("%a, %d %b %Y %H:%M:%S+0000", time.gmtime(time.time())).replace('  ', ' ')
                                  .replace(':',' ') + '.log', mode='w', encoding='UTF-8')
fileHandler.setLevel(logging.NOTSET)
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                              time.strftime("%a, %d %b %Y %H:%M:%S+0000", time.gmtime(time.time())))
consoleHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)
logger.addHandler(fileHandler)

uninstall_version="0.1"

def log(information):
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                                  time.strftime("%a, %d %b %Y %H:%M:%S+0000", time.gmtime(time.time())))
    consoleHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    logging.info(information)

def main():
    root = os.getenv("workDirectory")
    f = open(os.path.join(root, 'installed_version.txt'), 'r')
    install_version = f.read()
    f.close()
    #check whether the versions of uninstall script and install script are consistent.
    if install_version != uninstall_version:
        log("The version of the installation script and the uninstall script are inconsistent!")
    else:
        #uninstall windows services
        services = ['watchdog_service', 'app_config_manager_service', 'app_event_service', 'cleanup_service',
                    'event_daemon_service', 'monitor_screen_service', 'register_zookeeper_service', 'state_monitor_service',
                    'update_resource_service']
        service_names = ['AppCfgMgrService', 'AppeventService', 'CleanupService', 'EventDaemonService',
                         'ScreenMonitorService',
                         'RegisterZookeeperService', 'StateMonitorService', 'UpdateResourcesService', 'WatchdogService']
        dictionary = dict(zip(services, service_names))
        for service in services:
            if service == 'monitor_screen_service':
                os.system('TASKKILL /F /FI "services eq ScreenMonitorService"')
                log("Successfully stopped {service_name}".format(service_name=dictionary[service]))
            else:
                os.system('python -m gcp_wc.{service_name} stop'.format(service_name=service))
                log("Successfully stopped {service_name}".format(service_name=dictionary[service]))
            os.system('python -m gcp_wc.{service_name} remove'.format(service_name=service))
        log("Successfully removed {service_name}".format(service_name=dictionary[service]))

        #uninstall gcp-wc
        os.system('pip uninstall gcp-wc -y')
        log("Successfully uninstalled gcp-wc")

        #delete work directory
        shutil.rmtree(root)
        log("Successfully deleted work directory")

if __name__ == '__main__':
    main()
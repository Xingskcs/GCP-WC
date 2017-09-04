python -m gcp_wc.watchdog_service stop
python -m gcp_wc.app_config_manager_service stop
python -m gcp_wc.app_event_service stop
python -m gcp_wc.cleanup_service stop
python -m gcp_wc.event_daemon_service stop
python -m gcp_wc.register_zookeeper_service stop
python -m gcp_wc.state_monitor_service stop
python -m gcp_wc.update_resource_service stop
TASKKILL /F /FI "services eq ScreenMonitorService"

python -m gcp_wc.app_config_manager_service remove
python -m gcp_wc.app_event_service remove
python -m gcp_wc.cleanup_service remove
python -m gcp_wc.event_daemon_service remove
python -m gcp_wc.monitor_screen_service remove
python -m gcp_wc.register_zookeeper_service remove
python -m gcp_wc.state_monitor_service remove
python -m gcp_wc.update_resource_service remove
python -m gcp_wc.watchdog_service remove
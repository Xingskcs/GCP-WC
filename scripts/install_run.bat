python -m gcp_wc.app_config_manager_service install
python -m gcp_wc.app_event_service install
python -m gcp_wc.cleanup_service install
python -m gcp_wc.event_daemon_service install
python -m gcp_wc.monitor_screen_service install
python -m gcp_wc.register_zookeeper_service install
python -m gcp_wc.state_monitor_service install
python -m gcp_wc.update_resource_service install
python -m gcp_wc.watchdog_service install

python -m gcp_wc.watchdog_service start

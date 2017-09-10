import os
import time

time.sleep(60)
os.system('TASKKILL /F /FI "services eq EventDaemonService"')

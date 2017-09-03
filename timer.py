import os
import time

time.sleep(20)
os.system('TASKKILL /F /FI "services eq AppCfgMgrService"')

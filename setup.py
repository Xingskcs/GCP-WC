from distutils.core import setup

setup(
    name='GCP-WC',
    version='0.1',
    packages=['gcp-wc','gcp-wc/appcfg','gcp-wc/appcfgmgr','gcp-wc/appevents','gcp-wc/apptrace','gcp-wc/dirwatch','gcp-wc/event_daemon','gcp-wc/registerZookeeper','gcp-wc/statemonitor'],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
)
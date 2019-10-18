from setuptools import setup

setup(
    name='gcp_wc',
    version='0.1',
    packages=['gcp_wc'],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
    install_requires=[
        'chardet==3.0.4',
        'docker==2.4.2',
        'docker-pycreds==0.2.1',
        'Events==0.3',
        'idna==2.5',
        'kazoo==2.4.0',
        'psutil==5.2.2',
        'PyYAML==5.1',
        'requests==2.18.1',
        'six==1.10.0',
        'urllib3==1.21.1',
        'websocket-client==0.44.0',
    ],
    python_requires="~=3.5",
)

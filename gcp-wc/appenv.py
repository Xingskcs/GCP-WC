import os
import fs

class WindowsAppEnvironment():
    __slots__ = (
        'root',
        'cache_dir',
        'running_dir',
        'app_events_dir',
        'cleanup_dir'
    )
    CACHE_DIR = 'cache'
    RUNNING_DIR = 'running'
    APP_EVENTS_DIR = 'appevents'
    CLEANUP_DIR = 'cleanup'
    def __init__(self, root):
        self.root = root
        
        self.cache_dir = os.path.join(self.root, self.CACHE_DIR)
        self.running_dir = os.path.join(self.root, self.RUNNING_DIR)
        self.app_events_dir = os.path.join(self.root, self.APP_EVENTS_DIR)
        self.cleanup_dir = os.path.join(self.root, self.CLEANUP_DIR)

        fs.mkdir_safe(self.cache_dir)
        fs.mkdir_safe(self.running_dir)
        fs.mkdir_safe(self.app_events_dir)
        fs.mkdir_safe(self.cleanup_dir)
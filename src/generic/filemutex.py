# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
File mutex module
"""
import os
import stat
import time
import fcntl
from ovs_extensions.log.logger import Logger


class NoLockAvailableException(Exception):
    """
    Custom exception thrown when lock could not be acquired in due time
    """
    pass


class file_mutex(object):
    """
    This is mutex backed on the filesystem. It's cross thread and cross process. However
    its limited to the boundaries of a filesystem
    """

    def __init__(self, name, wait=None):
        """
        Creates a file mutex object
        """
        self.name = name
        self._has_lock = False
        self._start = 0
        self._logger = Logger('extensions')
        self._handle = open(self.key(), 'w')
        self._wait = wait
        try:
            os.chmod(
                self.key(),
                stat.S_IRUSR | stat.S_IWUSR |
                stat.S_IRGRP | stat.S_IWGRP |
                stat.S_IROTH | stat.S_IWOTH
            )
        except OSError:
            pass

    def __call__(self, wait):
        self._wait = wait
        return self

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        _ = args, kwargs
        self.release()

    def acquire(self, wait=None):
        """
        Acquire a lock on the mutex, optionally given a maximum wait timeout
        :param wait: Time to wait for lock
        """
        if self._has_lock:
            return True
        self._start = time.time()
        if wait is None:
            wait = self._wait
        passed = 0
        if wait is None:
            fcntl.flock(self._handle, fcntl.LOCK_EX)
            passed = time.time() - self._start
        else:
            while True:
                passed = time.time() - self._start
                if passed > wait:
                    self._logger.error('Lock for {0} could not be acquired. {1} sec > {2} sec'.format(self.key(), passed, wait))
                    raise NoLockAvailableException('Could not acquire lock %s' % self.key())
                try:
                    fcntl.flock(self._handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError:
                    time.sleep(0.005)
        if passed > 1:  # More than 1 s is a long time to wait!
            self._logger.warning('Waited {0} sec for lock {1}'.format(passed, self.key()))
        self._start = time.time()
        self._has_lock = True
        return True

    def release(self):
        """
        Releases the lock
        """
        if self._has_lock:
            fcntl.flock(self._handle, fcntl.LOCK_UN)
            passed = time.time() - self._start
            if passed > 2.5:  # More than 2.5 s is a long time to hold a lock
                self._logger.warning('A lock on {0} was kept for {1} sec'.format(self.key(), passed))
            self._has_lock = False

    def key(self):
        """
        Lock key
        """
        if '/' in self.name:
            return self.name  # Assuming a path
        return '/var/lock/ovs_flock_{0}'.format(self.name)

    def __del__(self):
        """
        __del__ hook, releasing the lock
        """
        self.release()
        self._handle.close()

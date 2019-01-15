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
Volatile mutex module
"""

import time
import logging


class NoLockAvailableException(Exception):
    """
    Custom exception thrown when lock could not be acquired in due time
    """
    pass


class volatile_mutex(object):
    """
    This is a volatile, distributed mutex to provide cross thread, cross process and cross node
    locking. However, this mutex is volatile and thus can fail. You want to make sure you don't
    lock for longer than a few hundred milliseconds to prevent this.
    """

    def __init__(self, name, wait=None):
        """
        Creates a volatile mutex object
        """
        self.name = name
        self._wait = wait
        self._start = 0
        self._logger = logging.getLogger(__name__)  # Overruled by classes inheriting
        self._has_lock = False
        self._volatile = self._get_volatile_client()

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
        while not self._volatile.add(self.key(), 1, 60):
            time.sleep(0.005)
            passed = time.time() - self._start
            if wait is not None and passed > wait:
                if self._logger is not None:
                    self._logger.error('Lock for {0} could not be acquired. {1} sec > {2} sec'.format(self.key(), passed, wait))
                raise NoLockAvailableException('Could not acquire lock {0}'.format(self.key()))
        passed = time.time() - self._start
        if passed > 0.2:  # More than 200 ms is a long time to wait
            if self._logger is not None:
                self._logger.warning('Waited {0} sec for lock {1}'.format(passed, self.key()))
        self._start = time.time()
        self._has_lock = True
        return True

    def release(self):
        """
        Releases the lock
        """
        if self._has_lock:
            self._volatile.delete(self.key())
            passed = time.time() - self._start
            if passed > 0.5:  # More than 500 ms is a long time to hold a lock
                if self._logger is not None:
                    self._logger.warning('A lock on {0} was kept for {1} sec'.format(self.key(), passed))
            self._has_lock = False

    def key(self):
        """
        Lock key
        """
        return 'ovs_lock_%s' % self.name

    def __del__(self):
        """
        __del__ hook, releasing the lock
        """
        self.release()

    @classmethod
    def _get_volatile_client(cls):
        raise NotImplementedError()

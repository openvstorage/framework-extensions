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
Arakoon store module, using pyrakoon
"""

import os
import uuid
import time
import ujson
import random
import logging
from threading import Lock, current_thread
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonClient, ArakoonClientConfig
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonNotFound, ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError, ArakoonAssertionFailed
from ovs_extensions.generic.repeatingtimer import RepeatingTimer

logger = logging.getLogger(__name__)


def locked():
    """
    Locking decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        def new_function(self, *args, **kw):
            """
            Executes the decorated function in a locked context
            """
            with self._lock:
                return f(self, *args, **kw)
        return new_function
    return wrap


class NoLockAvailableException(Exception):
    """
    Raised when the lock could not be acquired
    """
    pass


class PyrakoonClient(object):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """

    def __init__(self, cluster, nodes):
        """
        Initializes the client
        """
        cleaned_nodes = {}
        for node, info in nodes.iteritems():
            cleaned_nodes[str(node)] = ([str(entry) for entry in info[0]], int(info[1]))
        self._config = ArakoonClientConfig(str(cluster), cleaned_nodes)
        self._client = ArakoonClient(self._config, timeout=5, noMasterTimeout=5)
        self._identifier = int(round(random.random() * 10000000))
        self._lock = Lock()
        self._batch_size = 500
        self._sequences = {}

    @locked()
    def get(self, key, consistency=None):
        """
        Retrieves a certain value for a given key
        """
        return PyrakoonClient._try(self._identifier, self._client.get, key, consistency)

    @locked()
    def get_multi(self, keys, must_exist=True):
        """
        Get multiple keys at once
        """
        for item in PyrakoonClient._try(self._identifier,
                                        self._client.multiGet if must_exist is True else self._client.multiGetOption,
                                        keys):
            yield item

    @locked()
    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        if transaction is not None:
            return self._sequences[transaction].addSet(key, value)
        return PyrakoonClient._try(self._identifier, self._client.set, key, value,)

    @locked()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        next_prefix = PyrakoonClient._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = PyrakoonClient._try(self._identifier,
                                        self._client.range,
                                        beginKey=prefix if batch is None else batch[-1],
                                        beginKeyIncluded=batch is None,
                                        endKey=next_prefix,
                                        endKeyIncluded=False,
                                        maxElements=self._batch_size)
            for item in batch:
                yield item

    @locked()
    def prefix_entries(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        next_prefix = PyrakoonClient._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = PyrakoonClient._try(self._identifier,
                                        self._client.range_entries,
                                        beginKey=prefix if batch is None else batch[-1][0],
                                        beginKeyIncluded=batch is None,
                                        endKey=next_prefix,
                                        endKeyIncluded=False,
                                        maxElements=self._batch_size)
            for item in batch:
                yield item

    @locked()
    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            if must_exist is True:
                return self._sequences[transaction].addDelete(key)
            else:
                return self._sequences[transaction].addReplace(key, None)
        if must_exist is True:
            return PyrakoonClient._try(self._identifier, self._client.delete, key)
        else:
            return PyrakoonClient._try(self._identifier, self._client.replace, key, None)

    @locked()
    def delete_prefix(self, prefix):
        """
        Removes a given prefix from the store
        """
        return PyrakoonClient._try(self._identifier, self._client.deletePrefix, prefix)

    @locked()
    def nop(self):
        """
        Executes a nop command
        """
        return PyrakoonClient._try(self._identifier, self._client.nop)

    @locked()
    def exists(self, key):
        """
        Check if key exists
        """
        return PyrakoonClient._try(self._identifier, self._client.exists, key)

    @locked()
    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        if transaction is not None:
            return self._sequences[transaction].addAssert(key, value)
        return PyrakoonClient._try(self._identifier, self._client.aSSert, key, value)

    @locked()
    def assert_exists(self, key, transaction=None):
        """
        Asserts that a given key exists
        """
        if transaction is not None:
            return self._sequences[transaction].addAssertExists(key)
        return PyrakoonClient._try(self._identifier, self._client.aSSert_exists, key)

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        key = str(uuid.uuid4())
        self._sequences[key] = self._client.makeSequence()
        return key

    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        return PyrakoonClient._try(self._identifier, self._client.sequence, self._sequences[transaction], max_duration=1)

    @staticmethod
    def _try(identifier, method, *args, **kwargs):
        """
        Tries to call a given method, retry-ing if Arakoon is temporary unavailable
        """
        try:
            max_duration = 0.5
            if 'max_duration' in kwargs:
                max_duration = kwargs['max_duration']
                del kwargs['max_duration']
            start = time.time()
            try:
                return_value = method(*args, **kwargs)
            except (ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError):
                logger.debug('Error during arakoon call {0}, retry'.format(method.__name__))
                time.sleep(1)
                return_value = method(*args, **kwargs)
            duration = time.time() - start
            if duration > max_duration:
                logger.warning('Arakoon call {0} took {1}s'.format(method.__name__, round(duration, 2)))
            return return_value
        except (ArakoonNotFound, ArakoonAssertionFailed):
            # No extra logging for some errors
            raise
        except Exception:
            logger.error('Error during {0}. Process {1}, thread {2}, clientid {3}'.format(
                method.__name__, os.getpid(), current_thread().ident, identifier
            ))
            raise

    @staticmethod
    def _next_key(key):
        """
        Calculates the next key (to be used in range queries)
        """
        encoding = 'ascii'  # For future python 3 compatibility
        array = bytearray(str(key), encoding)
        for index in range(len(array) - 1, -1, -1):
            array[index] += 1
            if array[index] < 128:
                while array[-1] == 0:
                    array = array[:-1]
                return str(array.decode(encoding))
            array[index] = 0
        return '\xff'

    def lock(self, name, wait=None, expiration=60):
        # type: (str, float, float) -> PyrakoonLock
        """
        Returns the Arakoon lock implementation
        :param name: Name to give to the lock
        :type name: str
        :param wait: Wait time for the lock (in seconds)
        :type wait: float
        :param expiration: Expiration time for the lock (in seconds)
        :type expiration: float
        :return: The lock implementation
        :rtype: PyrakoonLock
        """
        return PyrakoonLock(self, name, wait, expiration)


class PyrakoonLock(object):
    """
    Lock implementation around Arakoon
    To be used as a context manager
    """
    LOCK_LOCATION = '/ovs/locks/{0}'
    EXPIRATION_KEY = 'expires'

    _logger = logging.getLogger('arakoon_lock')

    def __init__(self, client, name, wait=None, expiration=60):
        # type: (PyrakoonClient, str, float, float) -> None
        """
        Initialize a ConfigurationLock
        :param client: PyrakoonClient to work with
        :type client: PyrakoonClient
        :param name: Name of the lock to acquire.
        :type name: str
        :param expiration: Expiration time of the lock (in seconds)
        :type expiration: float
        :param wait: Amount of time to wait to acquire the lock (in seconds)
        :type wait: float
        """
        self.id = str(uuid.uuid4())
        self.name = name
        self._client = client
        self._expiration = expiration
        self._data_set = None
        self._key = self.LOCK_LOCATION.format(self.name)
        self._wait = wait
        self._start = 0
        self._has_lock = False
        self._refresher = None

    def __enter__(self):
        # type: () -> PyrakoonLock
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        # type: (*Any, **Any) -> None
        _ = args, kwargs
        self.release()

    def acquire(self, wait=None):
        # type: (float) -> bool
        """
        Acquire a lock on the mutex, optionally given a maximum wait timeout
        :param wait: Time to wait for lock
        :type wait: float
        """
        if self._has_lock:
            return True
        self._start = time.time()
        if wait is None:
            wait = self._wait
        while self._client.exists(self._key):
            time.sleep(0.005)
            # Check if it has expired
            try:
                original_lock_data = self._client.get(self._key)
                lock_data = ujson.loads(original_lock_data)
            except ArakoonNotFound:
                self._logger.debug('Unable to retrieve data: Key {0} was removed in meanwhile'.format(self._key))
                continue  # Key was removed in meanwhile
            except Exception:
                self._logger.exception('Unable to retrieve the data of key {0}'.format(self._key))
                continue
            expiration = lock_data.get(self.EXPIRATION_KEY, None)
            if expiration is None or time.time() > expiration:
                self._logger.info('Expiration for key {0} (lock id: {1}) was reached. Looking to remove it.'.format(self._key, lock_data['id']))
                # Remove the expired lock
                transaction = self._client.begin_transaction()
                self._client.assert_value(self._key, original_lock_data, transaction=transaction)
                self._client.delete(self._key, transaction=transaction)
                try:
                    self._client.apply_transaction(transaction)
                except ArakoonAssertionFailed:
                    self._logger.warning('Lost the race to cleanup the expired key {0}.'.format(self._key))
                except:
                    self._logger.exception('Unable to remove the expired entry')
                continue  # Always check the key again even when errors occurred
            passed = time.time() - self._start
            if wait is not None and passed > wait:
                self._logger.error('Lock for {0} could not be acquired. {1} sec > {2} sec'.format(self._key, passed, wait))
                raise NoLockAvailableException('Could not acquire lock {0}'.format(self._key))
        # Create the lock entry
        transaction = self._client.begin_transaction()
        self._client.assert_value(self._key, None, transaction=transaction)  # Key shouldn't exist
        data_to_set = self._get_lock_data()
        self._client.set(self._key, data_to_set, transaction=transaction)
        try:
            self._client.apply_transaction(transaction)
            self._data_set = data_to_set
            self._logger.debug('Acquired lock {0}'.format(self._key))
        except ArakoonAssertionFailed:
            self._logger.info('Lost the race with another lock, back to acquiring')
            return self.acquire(wait)
        except:
            self._logger.exception('Exception occurred while setting the lock')
            raise
        passed = time.time() - self._start
        if passed > 0.2:  # More than 200 ms is a long time to wait
            if self._logger is not None:
                self._logger.warning('Waited {0} sec for lock {1}'.format(passed, self._key))
        self._start = time.time()
        self._has_lock = True
        self._refresher = RepeatingTimer(5, self.refresh_lock)
        return True

    def _get_lock_data(self):
        now = time.time()
        return ujson.dumps({'time_set': now, self.EXPIRATION_KEY: now + self._expiration, 'id': self.id})

    def release(self):
        # type: () -> None
        """
        Releases the lock
        """
        if self._has_lock and self._data_set and self._refresher:
            self._refresher.cancel()
            self._refresher.join()
            transaction = self._client.begin_transaction()
            self._client.assert_value(self._key, self._data_set, transaction=transaction)
            self._client.delete(self._key, transaction=transaction)
            try:
                self._client.apply_transaction(transaction)
                self._logger.debug('Removed lock {0}'.format(self._key))
            except ArakoonAssertionFailed:
                self._logger.warning('The lock was removed and possible in use. Another client must have cleaned up the expired entry')
            except:
                self._logger.exception('Unable to remove the lock')
                raise
            passed = time.time() - self._start
            if passed > 0.5:  # More than 500 ms is a long time to hold a lock
                if self._logger is not None:
                    self._logger.warning('A lock on {0} was kept for {1} sec'.format(self._key, passed))
            self._has_lock = False

    def refresh_lock(self):
        """
        Refreshes the lock by setting now expiration dates
        """
        if self._has_lock and self._data_set is not None:
            transaction = self._client.begin_transaction()
            self._client.assert_value(self._key, self._data_set, transaction=transaction)
            data_to_set = self._get_lock_data()
            self._client.set(self._key, data_to_set, transaction=transaction)
            try:
                self._client.apply_transaction(transaction)
                self._data_set = data_to_set
                self._logger.debug('Refreshed lock {0}'.format(self._key))
            except ArakoonAssertionFailed:
                self._logger.exception('The lock was taken over by another instance')
                raise
            except:
                self._logger.exception('Unable to remove the lock')
                raise
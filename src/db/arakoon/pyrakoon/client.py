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
from functools import wraps
from threading import RLock, current_thread
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonAssertionFailed, ArakoonClient, ArakoonClientConfig, \
    ArakoonGoingDown, ArakoonNotFound, ArakoonNodeNotMaster, ArakoonNoMaster, ArakoonNotConnected, \
    ArakoonSocketException, ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError, Consistency
from ovs_extensions.generic.repeatingtimer import RepeatingTimer
from ovs_extensions.log.logger import Logger


def locked():
    """
    Locking decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        @wraps(f)
        def new_function(self, *args, **kw):
            """
            Executes the decorated function in a locked context
            """
            with self._lock:
                return f(self, *args, **kw)
        return new_function
    return wrap


def handle_arakoon_errors(is_read_only=False, max_duration=0.5, override_retry=False):
    # type: (bool, float) -> Any
    """
    - Handle that Arakoon can be unavailable
    - Handle master re-elections from Arakoon
        Only fetch requests are handled by this decorator
        Any update request must be attempted again by the client because it is unclear which part of the update was done eg.
        - Did the request reach the Arakoon server but it didn't reply
        - Did the request never reach the Arakoon server in the first place
    :param is_read_only: Indicate that the method is a read request
    :type is_read_only: bool
    :param max_duration: Maximum duration that a request should take. Logs a clear message that the request took longer when exceeded
    :type max_duration: float
    :param override_retry: Override retry. Used when opting to retry even if the read_only would be False.
    This might lead to an inconsistent state if the Arakoon goes down/master switches while processing the action
    Only use this option when rebuilding a transaction to assert the consistency
    :return: Result of underlying function
    :rtype: any
    """
    def wrap(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            # type: (PyrakoonClient, list, dict) -> any
            start = time.time()
            tries = 0
            retries = self._retries
            retry_back_off_multiplier = self._retry_back_off_multiplier
            retry_interval_sec = self._retry_interval_sec
            identifier = 'Process {0}, thread {1}, clientid {2}'.format(os.getpid(), current_thread().ident, self._identifier)
            try:
                while retries > tries:
                    try:
                        result = f(self, *args, **kwargs)
                        duration = time.time() - start
                        if duration > max_duration:
                            self._logger.warning('Pyrakoon call {0} took {1}s'.format(f.__name__, round(duration, 2)))
                        return result
                    except (ArakoonNoMaster, ArakoonNodeNotMaster, ArakoonSocketException, ArakoonNotConnected, ArakoonGoingDown) as ex:
                        # (ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError)  are the socket exception that can be retried
                        if not is_read_only and not override_retry and isinstance(ex, (ArakoonSocketException, ArakoonGoingDown)) and \
                                not isinstance(ex, (ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError)):
                            raise
                        # Drop all master connections and master related information
                        self._client._client_masterId = None
                        self._client.dropConnections()
                        sleep_time = retry_interval_sec * retry_back_off_multiplier ** tries
                        tries += 1.0
                        self._logger.warning("Master not found ({0}) during {1} ({2}). Retrying in {3:.2f} sec.".format(ex, f.__name__, identifier, sleep_time))
                        time.sleep(sleep_time)
            except (ArakoonNotFound, ArakoonAssertionFailed):
                # No extra logging for some errors
                raise
            except Exception:
                # Log any exception that might be thrown for debugging purposes
                self._logger.error('Error during {0}. {1}'.format(f.__name__, identifier))
                raise
        return wrapped
    return wrap


class NoLockAvailableException(Exception):
    """
    Raised when the lock could not be acquired
    """
    pass


class PyrakoonClient(object):
    """
    Arakoon client wrapper
    """
    _logger = Logger('extensions')

    def __init__(self, cluster, nodes, retries=10, retry_back_off_multiplier=2, retry_interval_sec=2):
        # type: (str, Dict[str, Tuple[str, int]], float, float) -> None
        """
        Initializes the client
        :param cluster: Identifier of the cluster
        :type cluster: str
        :param nodes: Dict with all node sockets. {name of the node: (ip of node, port of node)}
        :type nodes: dict
        :param retries: Number of retries to do
        :type retries: int
        :param retry_back_off_multiplier: Back off multiplier. Multiplies the retry_interval_sec with this number ** retry
        :type retry_back_off_multiplier: int
        :param retry_interval_sec: Seconds to wait before retrying. Exponentially increases with every retry.
        :type retry_interval_sec: int
        """
        cleaned_nodes = {}
        for node, info in nodes.iteritems():
            cleaned_nodes[str(node)] = ([str(entry) for entry in info[0]], int(info[1]))
        # Synchronization
        self._lock = RLock()
        # Wrapping
        self._config = ArakoonClientConfig(str(cluster), cleaned_nodes)
        self._client = ArakoonClient(self._config, timeout=5, noMasterTimeout=5)

        self._identifier = int(round(random.random() * 10000000))
        self._batch_size = 500
        self._sequences = {}
        # Retrying
        self._retries = retries
        self._retry_back_off_multiplier = retry_back_off_multiplier
        self._retry_interval_sec = retry_interval_sec

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def get(self, key, consistency=None):
        # type: (str, Consistency) -> Any
        """
        Retrieves a certain value for a given key
        :param key: The key whose value you are interested in
        :type key: str
        :param consistency: Consistency of the get
        :type consistency: Consistency
        :return: The value associated with the given key
        :rtype: any
        """
        return self._client.get(key, consistency)

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def get_multi(self, keys, must_exist=True):
        # type: (List[str], bool) -> Generator[Tuple[str, any]]
        """
        Get multiple keys at once
        :param keys: All keys to fetch
        :type keys" List[str]
        :param must_exist: Should all listed keys exist
        :type must_exist: bool
        :return: Generator that yields key value pairs
        :rtype: iterable[Tuple[str, any]
        """
        func = self._client.multiGet if must_exist is True else self._client.multiGetOption
        for item in func(keys):
            yield item

    @locked()
    @handle_arakoon_errors(is_read_only=False)
    def set(self, key, value, transaction=None):
        # type: (str, any, str) -> None
        """
        Sets the value for a key to a given value
        If the key does not yet have a value associated with it, a new key value pair will be created.
        If the key does have a value associated with it, it is overwritten.
        :param key: The key to set/update
        :type key: str
        :param value: The value to store
        :type value: any
        :param transaction: ID of the transaction to add the update too
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        if transaction is not None:
            return self._sequences[transaction].addSet(key, value)
        return self._client.set(key, value)

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def prefix(self, prefix):
        # type: (str) -> Generator[str]
        """
        Lists all keys starting with the given prefix
        :param prefix: Prefix of the key
        :type prefix: str
        :return: Generator that yields keys
        :rtype: iterable[str]
        """
        next_prefix = PyrakoonClient._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = self._client.range(beginKey=prefix if batch is None else batch[-1],
                                       beginKeyIncluded=batch is None,
                                       endKey=next_prefix,
                                       endKeyIncluded=False,
                                       maxElements=self._batch_size)
            for item in batch:
                yield item

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def prefix_entries(self, prefix):
        # type: (str) -> Generator[Tuple[str, any]]
        """
        Lists all key, value pairs starting with the given prefix
        :param prefix: Prefix of the key
        :type prefix: str
        :return: Generator that yields key, value pairs
        :rtype: iterable[Tuple[str, any]
        """
        next_prefix = PyrakoonClient._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = self._client.range_entries(beginKey=prefix if batch is None else batch[-1][0],
                                               beginKeyIncluded=batch is None,
                                               endKey=next_prefix,
                                               endKeyIncluded=False,
                                               maxElements=self._batch_size)
            for item in batch:
                yield item

    @locked()
    @handle_arakoon_errors(is_read_only=False)
    def delete(self, key, must_exist=True, transaction=None):
        # type: (str, bool, str) -> any
        """
        Deletes a given key from the store
        :param key; Key to remove
        ;type key: str
        :param must_exist: Should the key exist
        :type must_exist: bool
        :param transaction: Transaction to apply the update too
        :type transaction: id
        :return The previous value in case must_exist=False, None incase must_exist=False
        :rtype: any
        """
        if transaction is not None:
            if must_exist is True:
                return self._sequences[transaction].addDelete(key)
            else:
                return self._sequences[transaction].addReplace(key, None)
        if must_exist is True:
            return self._client.delete(key)
        else:
            return self._client.replace(key, None)

    @locked()
    @handle_arakoon_errors(is_read_only=False)
    def delete_prefix(self, prefix):
        # type: (str) -> None
        """
        Removes a given prefix from the store
        :param prefix: Prefix of the key
        :type prefix: str
        :return None
        ;:rtype: NoneType
        """
        return self._client.deletePrefix(prefix)

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def nop(self):
        # type: () -> None
        """
        Executes a nop command
        """
        return self._client.nop()

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def exists(self, key):
        # type: (str) -> bool
        """
        Check if key exists
        :param key: Key to check
        :type key: str
        :return True if key exists else False
        :rtype: bool
        """
        return self._client.exists(key)

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def assert_value(self, key, value, transaction=None):
        # type: (str, any, str) -> None
        """
        Asserts a key-value pair
        :param key: Key of the value to assert
        :type key: str
        :param value: Value to assert
        :type value: any
        :param transaction: Transaction to apply the assert too
        :type transaction: str
        :raises: ArakoonAssertionFailed if the value could not be asserted
        :return: None
        :rtype: NoneType
        """
        if transaction is not None:
            return self._sequences[transaction].addAssert(key, value)
        return self._client.aSSert(key, value)

    @locked()
    @handle_arakoon_errors(is_read_only=True)
    def assert_exists(self, key, transaction=None):
        # type: (str, str) -> None
        """
        Asserts that a given key exists
        :param key: Key to assert
        :type key: str
        :param transaction: Transaction to apply the assert too
        :type transaction: str
        :raises: ArakoonAssertionFailed if the value could not be asserted
        :return: None
        :rtype: NoneType
        """
        if transaction is not None:
            return self._sequences[transaction].addAssertExists(key)
        return self._client.aSSert_exists(key)

    def begin_transaction(self):
        # type: () -> str
        """
        Creates a transaction (wrapper around Arakoon sequences)
        :return: Identifier of the transaction
        :rtype: str
        """
        key = str(uuid.uuid4())
        self._sequences[key] = self._client.makeSequence()
        return key

    @locked()
    @handle_arakoon_errors(is_read_only=False, max_duration=1)
    def apply_transaction(self, transaction):
        # type: (str) -> None
        """
        Applies a transaction
        :param transaction: Identifier of the transaction
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        return self._client.sequence(self._sequences[transaction])

    @staticmethod
    def _next_key(key):
        # type: (str) -> str
        """
        Calculates the next key (to be used in range queries)
        :param key: Key to calucate of
        :type key: str
        :return: The next key
        :rtype: str
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

    @locked()
    def apply_callback_transaction(self, transaction_callback, max_retries=0, retry_wait_function=None):
        # type: (callable, int, callable) -> None
        """
        Apply a transaction which is the result of the callback.
        The callback should build the complete transaction again to handle the asserts. If the possible previous run was interrupted,
        the Arakoon might only have partially applied all actions therefore all asserts must be re-evaluated
        Handles all Arakoon errors by re-executing the callback until it finished or until no more retries can be made
        :param transaction_callback: Callback function which returns the transaction ID to apply
        :type transaction_callback: callable
        :param max_retries: Number of retries to try. Retries are attempted when an AssertException is thrown.
        Defaults to 0
        :param retry_wait_function: Function called retrying the transaction. The current try number is passed as an argument
        Defaults to lambda retry: time.sleep(randint(0, 25) / 100.0)
        :type retry_wait_function: callable
        :return: None
        :rtype: NoneType
        """
        # Apply transaction will retry on itself when connection failures happened
        # This callback function will retry on failures when the request was already sent
        # The callback aspect is required to re-evaluate the transaction
        @handle_arakoon_errors(is_read_only=False, max_duration=1, override_retry=True)
        def apply_callback_transaction(self):
            _ = self  # Self is added for the decorator
            # This inner function will execute the callback again on retry
            transaction = transaction_callback()

            self.apply_transaction(transaction)

        def default_retry_wait(retry):
            _ = retry
            time.sleep(random.randint(0, 25) / 100.0)

        retry_wait_func = retry_wait_function or default_retry_wait
        tries = 0
        success = False
        while success is False:
            tries += 1
            try:
                return apply_callback_transaction(self)
            except ArakoonAssertionFailed as ex:
                self._logger.warning('Asserting failed. Retrying {0} more times'.format(max_retries - tries))
                last_exception = ex
                if tries > max_retries:
                    raise last_exception
                retry_wait_func(tries)


class PyrakoonLock(object):
    """
    Lock implementation around Arakoon
    To be used as a context manager
    """
    LOCK_LOCATION = '/ovs/locks/{0}'
    EXPIRATION_KEY = 'expires'

    _logger = Logger('extensions')

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
        raise NotImplementedError('The Pyrakoon lease mechanism has not been properly implemented')
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

# Copyright (C) 2019 iNuron NV
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
import uuid
import time
import random
from .base_client import PyrakoonBase
from .client_pool import PyrakoonPool
from ovs_extensions.log.logger import Logger
from pyrakoon.compat import Sequence, ArakoonAssertionFailed, Consistency


class PyrakoonClientPooled(PyrakoonBase):
    """
    Pooled arakoon client wrapper
    Exposes the same API as the base PyrakoonClient while using a pool underneath

    NOTE: this client won't work in a thread except the main thread when not monkey patching gevent
    Monkey patching can be done using
    from gevent import monkey
    monkey.patch_all()

    This has to be called at the start of the application.
    """

    _logger = Logger('extensions')

    def __init__(self, cluster, nodes, pool_size=10, retries=10, retry_back_off_multiplier=2, retry_interval_sec=2):
        # type: (str, Dict[str, Tuple[str, int]], int, int, int, int) -> None
        """
        Initializes the client
        :param cluster: Identifier of the cluster
        :type cluster: str
        :param nodes: Dict with all node sockets. {name of the node: (ip of node, port of node)}
        :type nodes: dict
        :param pool_size: Number of clients to keep in the pool
        :type pool_size: int
        :param retries: Number of retries to do
        :type retries: int
        :param retry_back_off_multiplier: Back off multiplier. Multiplies the retry_interval_sec with this number ** retry
        :type retry_back_off_multiplier: int
        :param retry_interval_sec: Seconds to wait before retrying. Exponentially increases with every retry.
        :type retry_interval_sec: int
        """
        self._pool = PyrakoonPool(cluster, nodes, pool_size, retries, retry_back_off_multiplier, retry_interval_sec)
        self._sequences = {}

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
        with self._pool.get_client() as client:
            return client.get(key, consistency)

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
        with self._pool.get_client() as client:
            return client.get_multi(keys, must_exist=must_exist)

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
        with self._pool.get_client() as client:
            return client.set(key, value)

    def prefix(self, prefix):
        # type: (str) -> Generator[str]
        """
        Lists all keys starting with the given prefix
        :param prefix: Prefix of the key
        :type prefix: str
        :return: Generator that yields keys
        :rtype: iterable[str]
        """
        with self._pool.get_client() as client:
            return client.prefix(prefix)

    def prefix_entries(self, prefix):
        # type: (str) -> Generator[Tuple[str, any]]
        """
        Lists all key, value pairs starting with the given prefix
        :param prefix: Prefix of the key
        :type prefix: str
        :return: Generator that yields key, value pairs
        :rtype: iterable[Tuple[str, any]
        """
        with self._pool.get_client() as client:
            return client.prefix_entries(prefix)

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
        with self._pool.get_client() as client:
            return client.delete(key, must_exist)

    def delete_prefix(self, prefix, transaction=None):
        # type: (str, Optional[str]) -> None
        """
        Removes a given prefix from the store
        :param prefix: Prefix of the key
        :type prefix: str
        :param transaction: Transaction to apply the update too
        :type transaction: str
        :return None
        ;:rtype: NoneType
        """
        if transaction is not None:
            return self._sequences[transaction].addDeletePrefix(prefix)
        with self._pool.get_client() as client:
            return client.delete_prefix(prefix)

    def nop(self):
        # type: () -> None
        """
        Executes a nop command
        """
        with self._pool.get_client() as client:
            return client.nop()

    def exists(self, key):
        # type: (str) -> bool
        """
        Check if key exists
        :param key: Key to check
        :type key: str
        :return True if key exists else False
        :rtype: bool
        """
        with self._pool.get_client() as client:
            return client.exists(key)

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
        with self._pool.get_client() as client:
            return client.assert_value(key, value)

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
        with self._pool.get_client() as client:
            return client.assert_exists(key)

    def assert_range(self, prefix, keys, transaction):
        """
        Asserts that a given prefix yields the given keys
        :param prefix: Prefix of the key
        :type prefix: str
        :param keys: List of keys to assert
        :type keys: List[str]
        :param transaction: Transaction to apply the assert too
        :type transaction: str
        :raises: ArakoonAssertionFailed if the value could not be asserted
        :return: None
        :rtype: NoneType
        """
        return self._sequences[transaction].addAssertPrefixContainsExactly(prefix=prefix, keys=keys)

    def begin_transaction(self):
        # type: () -> str
        """
        Creates a transaction (wrapper around Arakoon sequences)
        :return: Identifier of the transaction
        :rtype: str
        """
        key = str(uuid.uuid4())
        self._sequences[key] = Sequence()
        return key

    def apply_transaction(self, transaction, delete=True):
        # type: (str, Optional[bool]) -> None
        """
        Applies a transaction
        :param transaction: Identifier of the transaction
        :type transaction: str
        :param delete: Delete transaction after attempting to apply the transaction
        Disabling this option requires a delete_transaction to be called at some point to avoid memory leaking
        :type delete: bool
        :return: None
        :rtype: NoneType
        """
        with self._pool.get_client() as client:
            try:
                sequence = self._sequences[transaction]
                return client._apply_transaction(sequence)
            finally:
                if delete:
                    self.delete_transaction(transaction)

    def delete_transaction(self, transaction):
        """
        Deletes a transaction
        :param transaction: Identifier of the transaction
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        self._sequences.pop(transaction, None)

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
        raise NotImplementedError('')

    def apply_callback_transaction(self, transaction_callback, max_retries=0, retry_wait_function=None):
        # type: (callable, int, callable) -> None
        """
        Apply a transaction which is the result of the callback.
        The callback should build the complete transaction again to handle the asserts. If the possible previous run was interrupted,
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
        def default_retry_wait(retry):
            _ = retry
            time.sleep(random.randint(0, 25) / 100.0)

        retry_wait_func = retry_wait_function or default_retry_wait
        tries = 0
        while True:
            tries += 1
            try:
                transaction = transaction_callback()  # type: str
                return self.apply_transaction(transaction)
            except ArakoonAssertionFailed as ex:
                self._logger.warning('Asserting failed. Retrying {0} more times'.format(max_retries - tries))
                last_exception = ex
                if tries > max_retries:
                    raise last_exception
                retry_wait_func(tries)

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

"""
Arakoon store module, using pyrakoon
"""

import copy
import time
import uuid
import ujson
import random
from threading import RLock
from .base_client import PyrakoonBase, locked
from pyrakoon.compat import ArakoonNotFound, ArakoonAssertionFailed
from ovs_extensions.generic.filemutex import file_mutex


class MockPyrakoonClient(PyrakoonBase):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """

    _data = {}
    _sequences = {}

    def __init__(self, cluster, nodes):
        """
        Initializes the client
        """
        _ = nodes
        self._lock = RLock()
        self._cluster = cluster
        if cluster not in self._sequences:
            self._sequences[cluster] = {}
        if cluster not in self._data:
            self._data[cluster] = {}

    def _read(self):
        return self._data.get(self._cluster, {})

    def _write(self, data):
        self._data[self._cluster] = data

    @locked()
    def get(self, key, consistency=None):
        """
        Retrieves a certain value for a given key
        """
        _ = consistency
        data = self._read()
        if key in data:
            return copy.deepcopy(data[key])
        else:
            raise ArakoonNotFound(key)

    @locked()
    def get_multi(self, keys, must_exist=True):
        """
        Get multiple keys at once
        """
        data = self._read()
        for key in keys:
            if key in data:
                yield copy.deepcopy(data[key])
            elif must_exist is True:
                raise ArakoonNotFound(key)
            else:
                yield None

    @locked()
    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.set, {'key': key, 'value': value}])
        data = self._read()
        data[key] = copy.deepcopy(value)
        self._write(data)

    @locked()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [k for k in data.keys() if k.startswith(prefix)]

    @locked()
    def prefix_entries(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [(k, v) for k, v in data.iteritems() if k.startswith(prefix)]

    @locked()
    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete, {'key': key, 'must_exist': must_exist}])
        data = self._read()
        if key in data:
            del data[key]
            self._write(data)
        elif must_exist is True:
            raise ArakoonNotFound(key)

    @locked()
    def delete_prefix(self, prefix, transaction=None):
        """
        Removes a given prefix from the store
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete_prefix, {'prefix': prefix}])
        data = self._read()
        keys_to_delete = [k for k in data if isinstance(k, str) and k.startswith(prefix)]
        for key in keys_to_delete:
            del data[key]
        if len(keys_to_delete) > 0:
            self._write(data)

    @locked()
    def nop(self):
        """
        Executes a nop command
        """
        pass

    @locked()
    def exists(self, key):
        """
        Check if key exists
        """
        try:
            self.get(key)
            return True
        except ArakoonNotFound:
            return False

    @locked()
    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_value, {'key': key, 'value': value}])
        data = self._read()
        if key not in data:
            raise ArakoonAssertionFailed(key)
        if ujson.dumps(data[key], sort_keys=True) != ujson.dumps(value, sort_keys=True):
            raise ArakoonAssertionFailed(key)

    @locked()
    def assert_exists(self, key, transaction=None):
        """
        Asserts that a given key exists
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_exists, {'key': key}])
        data = self._read()
        if key not in data:
            raise ArakoonAssertionFailed(key)

    def _sequence_assert_range(self, prefix, keys):
        """
        Asserts that a given prefix yields the given keys
        :param prefix: Prefix of the key
        :type prefix: str
        :param keys: List of keys to assert
        :type keys: List[str]
        :raises: ArakoonAssertionFailed if the value could not be asserted
        :return: None
        :rtype: NoneType
        """
        if keys != self.prefix(prefix):
            raise ArakoonAssertionFailed(prefix)

    @locked()
    def assert_range(self, prefix, keys, transaction):
        """
        Asserts that a given prefix yields the given keys
        Only usable with a transaction
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
        return self._sequences[transaction].append((self._sequence_assert_range, dict(prefix=prefix, keys=keys)))

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        key = str(uuid.uuid4())
        self._sequences[self._cluster][key] = []
        return key

    def apply_transaction(self, transaction, delete=True):
        """
        Applies a transaction
        """
        _ = delete
        begin_data = copy.deepcopy(self._read())  # Safer to copy than to reverse all actions
        for func, kwargs in self._sequences[transaction]:
            try:
                func(**kwargs)
            except Exception:
                self._write(begin_data)
                raise

    def apply_callback_transaction(self, transaction_callback, max_retries=0, retry_wait_function=None):
        # type: (callable, int, Optional[callable]) -> None
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
        def apply_callback_transaction():
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
                return apply_callback_transaction()
            except ArakoonAssertionFailed as ex:
                last_exception = ex
                if tries > max_retries:
                    raise last_exception
                retry_wait_func(tries)

    def lock(self, name, wait=None, expiration=60):
        # type: (str, float, float) -> file_mutex
        """
        Returns the Arakoon lock implementation
        :param name: Name to give to the lock
        :type name: str
        :param wait: Wait time for the lock (in seconds)
        :type wait: float
        :param expiration: Expiration time for the lock (in seconds)
        :type expiration: float
        :return: The lock implementation
        :rtype: ArakoonConfigurationLock
        """
        _ = expiration
        return file_mutex(name, wait)

    def delete_transaction(self, transaction):
        """
        Deletes a transaction
        :param transaction: Identifier of the transaction
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        self._sequences.pop(transaction, None)

    def _clean(self):
        """
        Clean the database
        :return: None
        """
        self._data[self._cluster] = {}

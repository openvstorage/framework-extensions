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
Dummy persistent module
"""
import os
import copy
import json
import time
import uuid
import random
from threading import RLock
from functools import wraps
from ovs_extensions.generic.filemutex import file_mutex
from ovs_extensions.storage.exceptions import KeyNotFoundException, AssertException
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonAssertionFailed, ArakoonNotFound


def synchronize():
    """
    Synchronization decorator
    """
    def wrap(f):
        """
        Wrapper function
        """
        @wraps(f)
        def new_function(self, *args, **kwargs):
            """
            Wrapped function
            """
            with self._lock:
                return f(self, *args, **kwargs)
        return new_function
    return wrap


class DummyPersistentStore(object):
    """
    This is a dummy persistent store that makes use of a local json file or memory
    When operating in memory mode: all functions that make use of _read() are already modifying the DB.
    This is intended but can lead to consistency problems (copy.deepcopy of the read is slow!)
    Can be used to substitute both PyrakoonStore and PyrakoonClient
    (this implementation does not enforce the JSON serialization like PyrakoonStore and implements all methods from PyrakoonClient)
    Note: when mimicking PyrakoonClient instead of store, set mimick_pyrakoonclient = True in the init so the same exceptions would be
    """
    def __init__(self, mimick_pyrakoonclient=False):
        self._data = {}
        self.id = str(uuid.uuid4())
        self._path = '/run/dummypersistent_{0}.json'.format(self.id)
        self._sequences = {}
        self._keep_in_memory_only = True
        self._lock = RLock()
        self.mimick_pyrakoonclient = mimick_pyrakoonclient

    @property
    def key_not_found_exception(self):
        """
        Get the appropriate KeyNotFoundException class
        """
        if self.mimick_pyrakoonclient is True:
            return ArakoonNotFound
        return KeyNotFoundException

    @property
    def assertion_exception(self):
        """
        Get the appropriate AssertionException class
        """
        if self.mimick_pyrakoonclient is True:
            return ArakoonAssertionFailed
        return AssertException

    @synchronize()
    def _clean(self):
        """
        Empties the store
        """
        if self._keep_in_memory_only is True:
            self._data = {}
        else:
            try:
                os.remove(self._path)
            except OSError:
                pass

    @synchronize()
    def _read(self):
        """
        Reads the local json file
        """
        if self._keep_in_memory_only is True:
            return self._data
        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {}
        return data

    @synchronize()
    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        data = self._read()
        if key in data:
            return copy.deepcopy(data[key])
        else:
            raise self.key_not_found_exception(key)

    @synchronize()
    def get_multi(self, keys, must_exist=True):
        """
        Retrieves values for all given keys
        """
        data = self._read()
        for key in keys:
            if key in data:
                yield copy.deepcopy(data[key])
            elif must_exist is True:
                raise self.key_not_found_exception(key)
            else:
                yield None

    @synchronize()
    def prefix(self, key):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [k for k in data.keys() if k.startswith(key)]

    @synchronize()
    def prefix_entries(self, key):
        """
        Returns all key-values starting with the given prefix
        """
        data = self._read()
        return [(k, copy.deepcopy(v)) for k, v in data.iteritems() if k.startswith(key)]

    @synchronize()
    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.set, {'key': key, 'value': copy.deepcopy(value)}])
        data = self._read()
        data[key] = copy.deepcopy(value)
        self._save(data)

    @synchronize()
    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete, {'key': key, 'must_exist': must_exist}])
        data = self._read()
        if key in data:
            del data[key]
            self._save(data)
        elif must_exist is True:
            raise self.key_not_found_exception(key)

    @synchronize()
    def delete_prefix(self, prefix, transaction=None):
        """
        Deletes all keys which start with the given prefix
        """
        if transaction is not None:
            raise NotImplementedError('Deleting prefix within a transaction is not possible')
        data = self._read()
        keys_to_delete = [k for k in data if isinstance(k, str) and k.startswith(prefix)]
        for key in keys_to_delete:
            del data[key]
        if len(keys_to_delete) > 0:
            self._save(data)

    @synchronize()
    def delete_prefix(self, prefix, transaction=None):
        """
        Deletes all keys which start with the given prefix
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete_prefix, {'prefix': prefix}])
        data = self._read()
        keys_to_delete = [k for k in data if isinstance(k, str) and k.startswith(prefix)]
        for key in keys_to_delete:
            del data[key]
        if len(keys_to_delete) > 0:
            self._save(data)

    @synchronize()
    def exists(self, key):
        """
        Check if key exists
        """
        try:
            self.get(key)
            return True
        except self.key_not_found_exception:
            return False

    @synchronize()
    def nop(self):
        """
        Executes a nop command
        """
        _ = self
        pass

    @synchronize()
    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_value, {'key': key, 'value': copy.deepcopy(value)}])
        data = self._read()
        if value is None:
            if key in data:
                raise self.assertion_exception(key)
        else:
            if key not in data:
                raise self.assertion_exception(key)
            if json.dumps(data[key], sort_keys=True) != json.dumps(value, sort_keys=True):
                raise self.assertion_exception(key)

    @synchronize()
    def assert_exists(self, key, transaction=None):
        """
        Asserts whether a given key exists
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_exists, {'key': key}])
        data = self._read()
        if key not in data:
            raise self.assertion_exception(key)

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        key = str(uuid.uuid4())
        self._sequences[key] = []
        return key

    @synchronize()
    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        begin_data = copy.deepcopy(self._read())  # Safer to copy than to reverse all actions
        for item in self._sequences[transaction]:
            try:
                item[0](**item[1])
            except Exception:
                self._save(begin_data)
                raise

    @synchronize()
    def _save(self, data):
        """
        Saves the local json file
        """
        if self._keep_in_memory_only is True:
            self._data = data
        else:
            f = open(self._path, 'w+')
            f.write(json.dumps(data, sort_keys=True, indent=2))
            f.close()

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

    def apply_callback_transaction(self, transaction_callback, max_retries=0, retry_wait_function=None):
        # type: (callable) -> None
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
            except AssertException as ex:
                last_exception = ex
                if tries > max_retries:
                    raise last_exception
                retry_wait_func(tries)

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
import ujson
from ConfigParser import RawConfigParser
from functools import wraps
from StringIO import StringIO
from ovs_extensions.db.arakoon.pyrakoon.client import PyrakoonClient, MockPyrakoonClient
from ovs_extensions.storage.exceptions import AssertException, KeyNotFoundException
from pyrakoon.compat import ArakoonAssertionFailed, ArakoonNotFound


def convert_exception():
    """
    Converting decorator.
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
            try:
                return f(self, *args, **kw)
            except ArakoonNotFound as field:
                raise KeyNotFoundException(field.message)
            except ArakoonAssertionFailed as assertion:
                raise AssertException(assertion)
        return new_function
    return wrap


class PyrakoonStore(object):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """
    def __init__(self, cluster, configuration):
        """
        Initializes the client
        """
        parser = RawConfigParser()
        parser.readfp(StringIO(configuration))
        nodes = {}
        for node in parser.get('global', 'cluster').split(','):
            node = node.strip()
            nodes[node] = ([parser.get(node, 'ip')], parser.get(node, 'client_port'))
        # @todo this code path is never hit due to the persistent factory getting the dummystore
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            self._client = MockPyrakoonClient(cluster, nodes)
        else:
            self._client = PyrakoonClient(cluster, nodes)

    @convert_exception()
    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return ujson.loads(self._client.get(key))
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored for {0}'.format(key))

    @convert_exception()
    def get_multi(self, keys, must_exist=True):
        """
        Get multiple keys at once
        """
        try:
            for item in self._client.get_multi(keys, must_exist=must_exist):
                yield None if item is None else ujson.loads(item)
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored')

    @convert_exception()
    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(key, ujson.dumps(value, sort_keys=True), transaction)

    @convert_exception()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        return self._client.prefix(prefix)

    @convert_exception()
    def prefix_entries(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        for item in self._client.prefix_entries(prefix):
            yield [item[0], ujson.loads(item[1])]

    @convert_exception()
    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        return self._client.delete(key, must_exist, transaction)

    @convert_exception()
    def delete_prefix(self, prefix, transaction=None):
        """
        Deletes all keys which start with the given prefix
        """
        return self._client.delete_prefix(prefix, transaction)

    @convert_exception()
    def nop(self):
        """
        Executes a nop command
        """
        return self._client.nop()

    @convert_exception()
    def exists(self, key):
        """
        Check if key exists
        """
        return self._client.exists(key)

    @convert_exception()
    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        return self._client.assert_value(key, None if value is None else ujson.dumps(value, sort_keys=True), transaction)

    @convert_exception()
    def assert_exists(self, key, transaction=None):
        """
        Asserts that a given key exists
        """
        return self._client.assert_exists(key, transaction)

    @convert_exception()
    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        return self._client.begin_transaction()

    @convert_exception()
    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        return self._client.apply_transaction(transaction)

    @convert_exception()
    def lock(self, name, wait=None, expiration=60):
        # type: (str, float, float) -> any
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
        return self._client.lock(name, wait, expiration)

    @convert_exception()
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
        return self._client.apply_callback_transaction(transaction_callback, max_retries, retry_wait_function)

    def _clean(self):
        # type: () -> None
        """
        Clean the database (if supported)
        :return: None
        """
        self._client._clean()

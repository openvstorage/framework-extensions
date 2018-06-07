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

import ujson
import logging
from ConfigParser import RawConfigParser
from StringIO import StringIO
from ovs_extensions.db.arakoon.pyrakoon.client import PyrakoonClient
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonAssertionFailed, ArakoonNotFound
from ovs_extensions.storage.exceptions import AssertException, KeyNotFoundException

logger = logging.getLogger(__name__)


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
        self._client = PyrakoonClient(cluster, nodes)

    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return ujson.loads(self._client.get(key))
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored for {0}'.format(key))
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field.message)

    def get_multi(self, keys, must_exist=True):
        """
        Get multiple keys at once
        """
        try:
            for item in self._client.get_multi(keys, must_exist=must_exist):
                yield None if item is None else ujson.loads(item)
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored')
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field.message)

    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(key, ujson.dumps(value, sort_keys=True), transaction)

    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        return self._client.prefix(prefix)

    def prefix_entries(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        for item in self._client.prefix_entries(prefix):
            yield [item[0], ujson.loads(item[1])]

    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        try:
            return self._client.delete(key, must_exist, transaction)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field.message)

    def nop(self):
        """
        Executes a nop command
        """
        return self._client.nop()

    def exists(self, key):
        """
        Check if key exists
        """
        return self._client.exists(key)

    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        try:
            return self._client.assert_value(key, None if value is None else ujson.dumps(value, sort_keys=True), transaction)
        except ArakoonAssertionFailed as assertion:
            raise AssertException(assertion)

    def assert_exists(self, key, transaction=None):
        """
        Asserts that a given key exists
        """
        try:
            return self._client.assert_exists(key, transaction)
        except ArakoonAssertionFailed as assertion:
            raise AssertException(assertion)

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        return self._client.begin_transaction()

    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        try:
            return self._client.apply_transaction(transaction)
        except ArakoonAssertionFailed as assertion:
            raise AssertException(assertion)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field.message)

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

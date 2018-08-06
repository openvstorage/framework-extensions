# Copyright (C) 2018 iNuron NV
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
import os
import time
from random import randint
from ovs_extensions.generic.configuration.clients.base import ConfigurationClientBase
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class ConfigurationBaseKeyValue(ConfigurationClientBase):
    """
    Client for Configuration Management with a key-value client
    Does key-splitting to fake a filesystem-alike system
    """

    def __init__(self, client, *args, **kwargs):
        """
        Initializes a Configuration client
        :param client: Underlying client to use
        """
        # type: (Any) -> None
        super(ConfigurationBaseKeyValue, self).__init__(*args, **kwargs)
        self._client = client

    def lock(self, name, wait=None, expiration=60):
        # type: (str, float, float) -> Any
        """
        Returns the lock implementation
        :param name: Name to give to the lock
        :type name: str
        :param wait: Wait time for the lock (in seconds)
        :type wait: float
        :param expiration: Expiration time for the lock (in seconds)
        :type expiration: float
        :return: The lock implementation
        :rtype: any
        """
        raise NotImplementedError()

    @property
    def assertion_exception(self):
        # type: () -> Any
        """
        Returns the used Exception class to indicate that an assertion failed
        :return: The underlying exception class
        """
        raise NotImplementedError()

    @property
    def key_not_found_exception(self):
        # type: () -> Any
        """
        Returns the use Exception class to indicate that a key was not found
        :return: The underlying exception class
        """
        raise NotImplementedError()

    def get_client(self):
        # type: () -> Any
        """
        Returns the underlying client
        :return: A client to maintain configurations
        :rtype: any
        """
        return self._client

    def get_configuration_path(self, key):
        # type: (str) -> str
        """
        Retrieve the full configuration path for specified key
        :param key: Key to retrieve full configuration path for
        :type key: str
        :return: Configuration path
        :rtype: str
        """
        raise NotImplementedError()

    @staticmethod
    def _clean_key(key):
        # type: (str) -> str
        """
        Cleans a key for key-value usage
        :param key: Key to clean
        :type key: str
        :return: Cleaned key
        :rtype: str
        """
        raise NotImplementedError()

    @classmethod
    def extract_key_from_path(cls, path):
        # type: (str) -> str
        """
        Extract a key from a path.
        Only used during testing as of now
        :param path: Path to extract the key from
        :type path: str
        :return: The extracted key
        :rtype: str
        """
        # Only available in unittests
        raise RuntimeError('Only available during unittests')

    def dir_exists(self, key):
        # type: (str) -> bool
        """
        Verify whether the directory exists
        :param key: Directory to check for existence
        :type key: str
        :return: True if directory exists, false otherwise
        :rtype: bool
        """
        key = self._clean_key(key)
        for entry in list(self._client.prefix(key)):
            parts = entry.split('/')
            for index in range(len(parts)):
                if key == '/'.join(parts[:index + 1]):
                    return self._client.exists(key) is False  # Exists returns False for directories (not complete keys)
        return False

    def list(self, key, recursive=False):
        # type: (str, bool) -> Generator[str]
        """
        List all keys starting with specified key
        :param key: Key to list
        :type key: str
        :param recursive: List keys recursively
        :type recursive: bool
        :return: Generator with all keys
        :rtype: generator
        """
        key = self._clean_key(key)
        entries = []
        for entry in self._client.prefix(key):
            if entry.startswith('_'):
                continue
            if recursive is True:
                parts = entry.split('/')
                for index, part in enumerate(parts):
                    if index == len(parts) - 1:  # Last part
                        yield entry  # Every entry is unique, so when having reached last part, we yield it
                    else:
                        dir_name = '{0}/'.format('/'.join(parts[:index + 1]))
                        if dir_name not in entries:
                            entries.append(dir_name)
                            yield dir_name
            else:
                if key == '' or entry.startswith(key.rstrip('/') + '/'):
                    cleaned = ExtensionsToolbox.remove_prefix(entry, key).strip('/').split('/')[0]
                    if cleaned not in entries:
                        entries.append(cleaned)
                        yield cleaned

    def delete(self, key, recursive, transaction=None):
        # type: (str, bool, str) -> None
        """
        Delete the specified key
        :param key: Key to delete
        :type key: str
        :param recursive: Delete the specified key recursively
        :type recursive: bool
        :param transaction: Transaction to apply the delete too
        :type transaction: str
        :return: None
        """
        key = self._clean_key(key)
        if recursive is True:
            if transaction is not None:
                raise NotImplementedError('Deleting recursively within a transaction is not possible')
            self._client.delete_prefix(key, transaction=transaction)
        else:
            self._client.delete(key, transaction=transaction)

    def get(self, key, **kwargs):
        # type: (str, **kwargs) -> str
        """
        Retrieve the value for specified key
        :param key: Key to retrieve
        :type key: str
        :return: Value of key
        :rtype: str
        """
        key = self._clean_key(key)
        return self._client.get(key, **kwargs)

    def set(self, key, value, transaction=None):
        # type: (str, str, str) -> None
        """
        Set a value for specified key
        :param key: Key to set
        :type key: str
        :param value: Value to set for key
        :type value: str
        param transaction: Transaction to apply the delete too
        :type transaction: str
        :return: None
        """
        if isinstance(value, basestring):
            value = str(value)
        key = self._clean_key(key)
        self._client.set(key, value, transaction=transaction)

    def rename(self, key, new_key, max_retries=20):
        # type: (str, str, int) -> None
        """
        Rename a path
        :param key: Start of the path to rename
        :type key: str
        :param new_key: New key value
        :type new_key: str
        :param max_retries: Number of retries to attempt
        :type max_retries: int
        :return: None
        :rtype: NoneType
        :raises AssertException: when the assertion failed after 'max_retries' times
        """
        key = self._clean_key(key)
        new_key = self._clean_key(new_key)
        tries = 0
        success = False
        last_exception = None
        while success is False:
            tries += 1
            if tries > max_retries:
                raise last_exception

            transaction = self._client.begin_transaction()
            for entry, entry_value in self._client.prefix_entries(key):
                # Handle case where the entry only startswith key. Should not be renamed
                # Ideally os.path.realpath can be used but this might follow links towards other files on the real filesystem
                # @todo implement
                if entry == key:  # Handle rename of the exact key
                    new_key_entry = new_key
                else:
                    if entry.replace(key, '').startswith('/'):
                        entry_suffix = os.path.relpath(entry, key)
                        new_key_entry = os.path.join(new_key, entry_suffix)
                    else:
                        continue
                self._client.assert_value(entry, entry_value, transaction=transaction)  # The value of the key should not have changed
                self._client.set(new_key_entry, entry_value, transaction=transaction)
                self._client.delete(entry, transaction=transaction)
            try:
                self._client.apply_transaction(transaction)
                success = True
            except self.assertion_exception as ex:
                last_exception = ex
                time.sleep(randint(0, 25) / 100.0)
        if success is False:
            self._logger.exception('Transaction for rename failed: last warning was "{0}"'.format(last_exception))

    def assert_value(self, key, value, transaction=None):
        # type: (str, Any, str) -> None
        """
        Asserts a key-value pair
        :param key: Key to assert for
        :type key: str
        :param value: Value that the key should have
        :type value: any
        :param transaction: Transaction to apply this action too
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        return self._client.assert_value(key, value, transaction=transaction)

    def assert_exists(self, key, transaction=None):
        # type: (str, str) -> None
        """
        Asserts that a key exists
        :param key: Key to assert for
        :type key: str
        :param transaction: Transaction to apply this action too
        :type transaction: str
        :return: None
        :rtype: NoneType
        """
        return self._client.assert_exists(key, transaction=transaction)

    def begin_transaction(self):
        # type: () -> str
        """
        Starts a new transaction. All actions which support transactions can be used with this identifier
        :return: The ID of the started transaction
        :rtype: str
        """
        return self._client.begin_transaction()

    def apply_transaction(self, transaction):
        # type: (str) -> None
        """
        Applies a transaction. All registered actions are executed
        :param transaction: Transaction to apply
        :type transaction: str
        :return: None
        :rtype: NoneType
        :raises assertion_exception: when an assert failure was reached
        :raises key_not_found_exception: when a key could not be found
        """
        return self._client.apply_transaction(transaction)

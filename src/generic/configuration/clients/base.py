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

"""
Dummy persistent module
"""
import logging


class ConfigurationClientBase(object):
    """
    Base for all ConfigurationClients.
    These are built and used by the Configuration abstraction.
    Configuration is an abstraction of a filesystem-alike configuration management like ETCD.
    All inheriting classes must overrule lock, get_configuration_path, extract_key_from_path, get, set, dir_exists, list, delete, rename
    """

    _logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        _ = args, kwargs

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

    def dir_exists(self, key):
        # type: (str) -> bool
        """
        Returns if the given directory of the key exists
        :param key: Key to check for
        :type key: str
        :return: True when key exists else False
        :rtype: bool
        """
        # type: (str) -> bool
        raise NotImplementedError()

    def list(self, key, recursive):
        # type: (str, bool) -> Generator[str]
        """
        Lists all contents under the key.
        :param key: Key to list under
        :type key: str
        :param recursive: Indicate to list recursively
        :type recursive: bool
        :return: All contents under the list
        :rtype: Iterable
        """
        # type: (str, bool) -> Iterable(str)
        raise NotImplementedError()

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
        raise NotImplementedError()

    def get(self, key, **kwargs):
        # type: (str, **kwargs) -> Union[dict, None]
        """
        Retrieve the value for specified key
        :param key: Key to retrieve
        :type key: str
        :return: Value of key
        :rtype: str
        """
        raise NotImplementedError()

    def get_client(self):
        # type: () -> Any
        """
        Returns the underlying client
        :return: A client to maintain configurations
        :rtype: any
        """
        raise NotImplementedError()

    def set(self, key, value, transaction=None):
        # type: (str, any) -> None
        """
        Set a value for specified key
        :param key: Key to set
        :type key: str
        :param value: Value to set for key
        :type value: str
        :param transaction: Transaction to apply the delete too
        :type transaction: str
        :return: None
        """
        raise NotImplementedError()

    def rename(self, key, new_key, max_retries):
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
        raise NotImplementedError()

    def begin_transaction(self):
        # type: () -> str
        """
        Starts a new transaction. All actions which support transactions can be used with this identifier
        :return: The ID of the started transaction
        :rtype: str
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    @classmethod
    def get_configuration_path(cls, key):
        # type: (str) -> str
        """
        Retrieve the full configuration path for specified key
        :param key: Key to retrieve full configuration path for
        :type key: str
        :return: Configuration path
        :rtype: str
        """
        raise NotImplementedError()

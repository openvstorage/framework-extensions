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
from ovs_extensions.constants.config import CACC_LOCATION
from ovs_extensions.generic.configuration.clients.base_keyvalue import ConfigurationBaseKeyValue
from ovs_extensions.generic.filemutex import file_mutex
from ovs_extensions.storage.persistent.dummystore import DummyPersistentStore


class ConfigurationMockKeyValue(ConfigurationBaseKeyValue):
    """
    This is a dummy configuration store that makes use of a local json file
    Uses the DummyPersistentStore as underlying storage
    """

    def __init__(self, *args, **kwargs):
        client = DummyPersistentStore()
        client._path = '/run/dummyconfiguration.json'
        super(ConfigurationMockKeyValue, self).__init__(client, *args, **kwargs)

    @property
    def assertion_exception(self):
        # type: () -> Any
        """
        Returns the used Exception class to indicate that an assertion failed
        :return: The underlying exception class
        """
        return self._client.assertion_exception

    @property
    def key_not_found_exception(self):
        """
        Returns the use Exception class to indicate that a key was not found
        :return: The underlying exception class
        """
        return self._client.key_not_found_exception

    def lock(self, name, wait=None, expiration=60):
        """
        Returns the file mutex implementation
        :param name: Name to give to the lock
        :type name: str
        :param wait: Wait time for the lock (in seconds)
        :type wait: float
        :param expiration: Expiration time for the lock (in seconds)
        :type expiration: float
        :return: The lock implementation
        :rtype: ArakoonConfigurationLock
        """
        return file_mutex(name, wait)

    @classmethod
    def _clean_key(cls, key):
        # type: (str) -> (str)
        """
        Cleans a key. Strips off all beginning and ending '/'
        :param key: Key to clean
        :return: Cleaned key
        """
        return key.strip('/')

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
        return path.split('=')[-1]

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
        return 'file://{0}?key={1}'.format(CACC_LOCATION, key)

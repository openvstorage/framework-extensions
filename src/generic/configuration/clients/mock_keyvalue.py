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
        return self._client.assertion_exception

    def lock(self, name, wait=None, expiration=60):
        return file_mutex(name, wait)

    @classmethod
    def _clean_key(cls, key):
        """
        Cleans a key. Strips off all beginning and ending '/'
        :param key: Key to clean
        :return: Cleaned key
        """
        return key.strip('/')

    @classmethod
    def extract_key_from_path(cls, path):
        return path.split('=')[-1]

    @classmethod
    def get_configuration_path(cls, key):
        return 'file://opt/OpenvStorage/config/framework.json?key={0}'.format(key)

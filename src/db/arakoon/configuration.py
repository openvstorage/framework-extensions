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
Generic module for managing configuration in Arakoon
"""
import os
import time
from ConfigParser import RawConfigParser
from ovs_extensions.log.logger import Logger
from ovs_extensions.storage.exceptions import AssertException
from ovs_extensions.db.arakoon.pyrakoon.client import PyrakoonClient
from random import randint


class ArakoonConfiguration(object):
    """
    Helper for configuration management in Arakoon
    """
    client = None

    def __init__(self, cacc_location):
        # type: (str) -> None
        self._client = None
        self.cacc_location = cacc_location
        self._logger = Logger('extensions')

    def get_configuration_path(self, key):
        # type: (str) -> str
        """
        Retrieve the full configuration path for specified key
        :param key: Key to retrieve full configuration path for
        :type key: str
        :return: Configuration path
        :rtype: str
        """
        import urllib
        parser = RawConfigParser()
        with open(self.cacc_location) as config_file:
            parser.readfp(config_file)
        cluster_id = parser.get('global', 'cluster_id')
        return 'arakoon://{0}/{1}?{2}'.format(
            cluster_id,
            ArakoonConfiguration._clean_key(key),
            urllib.urlencode({'ini': self.cacc_location})
        )

    def dir_exists(self, key):
        # type: (str) -> bool
        """
        Verify whether the directory exists
        :param key: Directory to check for existence
        :type key: str
        :return: True if directory exists, false otherwise
        :rtype: bool
        """
        key = ArakoonConfiguration._clean_key(key)
        client = self.get_client()
        for entry in list(client.prefix(key)):
            parts = entry.split('/')
            for index in range(len(parts)):
                if key == '/'.join(parts[:index + 1]):
                    return client.exists(key) is False  # Exists returns False for directories (not complete keys)
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
        from ovs_extensions.generic.toolbox import ExtensionsToolbox

        key = ArakoonConfiguration._clean_key(key)
        client = self.get_client()
        entries = []
        for entry in client.prefix(key):
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

    def delete(self, key, recursive):
        # type: (str, bool) -> None
        """
        Delete the specified key
        :param key: Key to delete
        :type key: str
        :param recursive: Delete the specified key recursively
        :type recursive: bool
        :return: None
        """
        key = ArakoonConfiguration._clean_key(key)
        client = self.get_client()
        if recursive is True:
            client.delete_prefix(key)
        else:
            client.delete(key)

    def get(self, key, **kwargs):
        # type: (str, **kwargs) -> str
        """
        Retrieve the value for specified key
        :param key: Key to retrieve
        :type key: str
        :return: Value of key
        :rtype: str
        """
        key = ArakoonConfiguration._clean_key(key)
        client = self.get_client()
        return client.get(key, **kwargs)

    def set(self, key, value):
        # type: (str, str) -> None
        """
        Set a value for specified key
        :param key: Key to set
        :type key: str
        :param value: Value to set for key
        :type value: str
        :return: None
        """
        if isinstance(value, basestring):
            value = str(value)
        key = ArakoonConfiguration._clean_key(key)
        client = self.get_client()
        client.set(key, value)

    def get_client(self):
        # type: () -> PyrakoonClient
        """
        Builds a PyrakoonClient
        :return: A PyrakoonClient instance
        :rtype: ovs_extensions.db.arakoon.pyrakoon.client.PyrakoonClient
        """
        if self._client is None:
            parser = RawConfigParser()
            with open(self.cacc_location) as config_file:
                parser.readfp(config_file)
            nodes = {}
            for node in parser.get('global', 'cluster').split(','):
                node = node.strip()
                nodes[node] = ([parser.get(node, 'ip')], parser.get(node, 'client_port'))
            self._client = PyrakoonClient(parser.get('global', 'cluster_id'), nodes)
        return self._client

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
        client = self.get_client()
        key = self._clean_key(key)
        new_key = self._clean_key(new_key)
        tries = 0
        success = False
        last_exception = None
        while success is False:
            tries += 1
            if tries > max_retries:
                raise last_exception

            transaction = client.begin_transaction()
            for entry, entry_value in client.prefix_entries(key):
                entry_suffix = os.path.relpath(entry, key)
                new_key_entry = os.path.join(new_key, entry_suffix)
                client.assert_value(entry, entry_value, transaction=transaction)  # The value of the key should not have changed
                client.set(new_key_entry, entry_value, transaction=transaction)
                client.delete(entry, transaction=transaction)
                client.assert_value(new_key_entry, entry_value, transaction=transaction)  # The value of the new key should not have changed
            try:
                client.apply_transaction(transaction)
                success = True
            except AssertException as ex:
                last_exception = ex
                time.sleep(randint(0, 25) / 100.0)
        if success is False:
            self._logger.exception('Transaction for rename failed: last warning was"{0}"'.format(last_exception))


    @staticmethod
    def _clean_key(key):
        # type: (str) -> str
        return key.lstrip('/')

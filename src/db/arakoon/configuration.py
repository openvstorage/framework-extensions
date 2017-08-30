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

from ConfigParser import RawConfigParser
from ovs_extensions.db.arakoon.pyrakoon.client import PyrakoonClient


class ArakoonConfiguration(object):
    """
    Helper for configuration management in Arakoon
    """
    client = None

    def __init__(self, cacc_location):
        self._client = None
        self.cacc_location = cacc_location

    def get_configuration_path(self, key):
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

    @staticmethod
    def _clean_key(key):
        return key.lstrip('/')

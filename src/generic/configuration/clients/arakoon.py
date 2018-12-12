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
import re
import uuid
import time
import ujson
import urllib
from ConfigParser import RawConfigParser
from ovs_extensions.generic.configuration.clients.base_keyvalue import ConfigurationBaseKeyValue
from ovs_extensions.log.logger import Logger
from ovs_extensions.generic.configuration import NoLockAvailableException
from ovs_extensions.db.arakoon.pyrakoon.client import PyrakoonClient, ArakoonAssertionFailed, ArakoonNotFound


class ArakoonConfiguration(ConfigurationBaseKeyValue):
    """
    Client for Configuration Management in Arakoon
    """

    def __init__(self, cacc_location, *args, **kwargs):
        # type: (str) -> None
        self.cacc_location = cacc_location
        # Build a client
        super(ArakoonConfiguration, self).__init__(self.get_client(), *args, **kwargs)

    @property
    def assertion_exception(self):
        # type: () -> type[ArakoonAssertionFailed]
        """
        Returns the used Exception class to indicate that an assertion failed
        :return: The underlying exception class
        :rtype: type[ArakoonAssertionFailed]
        """
        return ArakoonAssertionFailed

    @property
    def key_not_found_exception(self):
        # type: () -> type[ArakoonNotFound]
        """
        Returns the use Exception class to indicate that a key was not found
        :return: The underlying exception class
        :rtype: type[ArakoonNotFound]
        """
        return ArakoonNotFound

    def lock(self, name, wait=None, expiration=60):
        # type: (str, float, float) -> ArakoonConfigurationLock
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
        return ArakoonConfigurationLock(self.cacc_location, name, wait, expiration)

    def get_configuration_path(self, key):
        # type: (str) -> str
        """
        Retrieve the full configuration path for specified key
        :param key: Key to retrieve full configuration path for
        :type key: str
        :return: Configuration path
        :rtype: str
        """
        parser = RawConfigParser()
        with open(self.cacc_location) as config_file:
            parser.readfp(config_file)
        cluster_id = parser.get('global', 'cluster_id')
        return 'arakoon://{0}/{1}?{2}'.format(
            cluster_id,
            ArakoonConfiguration._clean_key(key),
            urllib.urlencode({'ini': self.cacc_location})
        )

    def get_client(self):
        # type: () -> PyrakoonClient
        """
        Builds a PyrakoonClient
        :return: A PyrakoonClient instance
        :rtype: ovs_extensions.db.arakoon.pyrakoon.client.PyrakoonClient
        """
        parser = RawConfigParser()
        with open(self.cacc_location) as config_file:
            parser.readfp(config_file)
        nodes = {}
        for node in parser.get('global', 'cluster').split(','):
            node = node.strip()
            nodes[node] = ([parser.get(node, 'ip')], parser.get(node, 'client_port'))
        return PyrakoonClient(parser.get('global', 'cluster_id'), nodes)

    @staticmethod
    def _clean_key(key):
        # type: (str) -> str
        """
        Cleans a key for Arakoon usage
        :param key: Key to clean
        :type key: str
        :return: Cleaned key
        :rtype: str
        """
        return key.lstrip('/')

    @classmethod
    def extract_key_from_path(cls, path):
        # type: (str) -> str
        """
        Extract a key from a path.
        :param path: Path to extract the key from
        :type path: str
        :return: The extracted key
        :rtype: str
        """
        # Only available in unittests
        r = re.compile("arakoon://(?:\w*/)"  # Arakoon prefix + cluster id
                       "(.*?)"  # Captured group
                       "\?ini=.*")  # Cacc ini
        match = r.match(path)
        return match.group(1)



class ArakoonConfigurationLock(object):
    """
    Lock implementation around Arakoon
    To be used as a context manager
    """
    LOCK_LOCATION = '/ovs/locks/{0}'
    EXPIRATION_KEY = 'expires'

    _logger = Logger('arakoon_configuration_lock')

    def __init__(self, cacc_location, name, wait=None, expiration=60):
        # type: (str, str, float, float) -> None
        """
        Initialize a ConfigurationLock
        :param cacc_location: Path to the the configuration file
        :type cacc_location: str
        :param name: Name of the lock to acquire.
        :type name: str
        :param expiration: Expiration time of the lock (in seconds)
        :type expiration: float
        :param wait: Amount of time to wait to acquire the lock (in seconds)
        :type wait: float
        """
        self.id = str(uuid.uuid4())
        self.name = name
        self._cacc_location = cacc_location
        config = ArakoonConfiguration(self._cacc_location)
        self._client = config.get_client()
        self._expiration = expiration
        self._data_set = None
        self._key = self.LOCK_LOCATION.format(self.name)
        self._wait = wait
        self._start = 0
        self._has_lock = False

    def __enter__(self):
        # type: () -> ArakoonConfigurationLock
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        # type: (*Any, **Any) -> None
        _ = args, kwargs
        self.release()

    def acquire(self, wait=None):
        # type: (float) -> bool
        """
        Acquire a lock on the mutex, optionally given a maximum wait timeout
        :param wait: Time to wait for lock
        :type wait: float
        """
        if self._has_lock:
            return True
        self._start = time.time()
        if wait is None:
            wait = self._wait
        while self._client.exists(self._key):
            time.sleep(0.005)
            # Check if it has expired
            try:
                original_lock_data = self._client.get(self._key)
                lock_data = ujson.loads(original_lock_data)
            except ArakoonNotFound:
                self._logger.debug('Unable to retrieve data: Key {0} was removed in meanwhile'.format(self._key))
                continue  # Key was removed in meanwhile
            except Exception:
                self._logger.exception('Unable to retrieve the data of key {0}'.format(self._key))
                continue
            expiration = lock_data.get(self.EXPIRATION_KEY, None)
            if expiration is None or time.time() > expiration:
                self._logger.info('Expiration for key {0} (lock id: {1}) was reached. Looking to remove it.'.format(self._key, lock_data['id']))
                # Remove the expired lock
                transaction = self._client.begin_transaction()
                self._client.assert_value(self._key, original_lock_data, transaction=transaction)
                self._client.delete(self._key, transaction=transaction)
                try:
                    self._client.apply_transaction(transaction)
                except ArakoonAssertionFailed:
                    self._logger.warning('Lost the race to cleanup the expired key {0}.'.format(self._key))
                except:
                    self._logger.exception('Unable to remove the expired entry')
                continue  # Always check the key again even when errors occurred
            passed = time.time() - self._start
            if wait is not None and passed > wait:
                self._logger.error('Lock for {0} could not be acquired. {1} sec > {2} sec'.format(self._key, passed, wait))
                raise NoLockAvailableException('Could not acquire lock {0}'.format(self._key))
        # Create the lock entry
        now = time.time()
        transaction = self._client.begin_transaction()
        self._client.assert_value(self._key, None, transaction=transaction)  # Key shouldn't exist
        data_to_set = ujson.dumps({'time_set': now, self.EXPIRATION_KEY: now + self._expiration, 'id': self.id})
        self._client.set(self._key, data_to_set, transaction=transaction)
        try:
            self._client.apply_transaction(transaction)
            self._data_set = data_to_set
            self._logger.debug('Acquired lock {0}'.format(self._key))
        except ArakoonAssertionFailed:
            self._logger.info('Lost the race with another lock, back to acquiring')
            return self.acquire(wait)
        except:
            self._logger.exception('Exception occurred while setting the lock')
            raise
        passed = time.time() - self._start
        if passed > 0.2:  # More than 200 ms is a long time to wait
            if self._logger is not None:
                self._logger.warning('Waited {0} sec for lock {1}'.format(passed, self._key))
        self._start = time.time()
        self._has_lock = True
        return True

    def release(self):
        # type: () -> None
        """
        Releases the lock
        """
        if self._has_lock and self._data_set is not None:
            transaction = self._client.begin_transaction()
            self._client.assert_value(self._key, self._data_set, transaction=transaction)
            self._client.delete(self._key, transaction=transaction)
            try:
                self._client.apply_transaction(transaction)
                self._logger.debug('Removed lock {0}'.format(self._key))
            except ArakoonAssertionFailed:
                self._logger.warning('The lock was removed and possible in use. Another client must have cleaned up the expired entry')
            except:
                self._logger.exception('Unable to remove the lock')
                raise
            passed = time.time() - self._start
            if passed > 0.5:  # More than 500 ms is a long time to hold a lock
                if self._logger is not None:
                    self._logger.warning('A lock on {0} was kept for {1} sec'.format(self._key, passed))
            self._has_lock = False

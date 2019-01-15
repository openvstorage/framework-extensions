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
Generic module for managing configuration somewhere
"""
import os
import sys
import json
import time
import collections
from random import randint
from subprocess import check_output
from ovs_extensions.constants.config import CACC_LOCATION, COMPONENTS_KEY
from ovs_extensions.constants.file_extensions import RAW_FILES
from ovs_extensions.generic.system import System
from ovs_extensions.packages.packagefactory import PackageFactory
from ovs_extensions.log.logger import Logger
# Import for backwards compatibility/easier access
from ovs_extensions.generic.configuration.exceptions import ConfigurationNotFoundException as NotFoundException
from ovs_extensions.generic.configuration.exceptions import ConfigurationAssertionException  # New exception, not mapping


class Configuration(object):
    """
    Configuration wrapper.
    Uses a special key format to specify the path within the configuration store, and specify a path inside the json data
    object that might be stored inside the key.
    key  = <main path>[|<json path>]
    main path = slash-delimited path
    json path = dot-delimited path
    Examples:
        > Configuration.set('/foo', 1)
        > print Configuration.get('/foo')
        < 1
        > Configuration.set('/foo', {'bar': 1})
        > print Configuration.get('/foo')
        < {u'bar': 1}
        > print Configuration.get('/foo|bar')
        < 1
        > Configuration.set('/bar|a.b', 'test')
        > print Configuration.get('/bar')
        < {u'a': {u'b': u'test'}}
    """

    BASE_KEY = '/ovs/framework'
    CACC_LOCATION = CACC_LOCATION
    EDITION_KEY = '{0}/edition'.format(BASE_KEY)

    _clients = {}
    _logger = Logger('extensions')

    def __init__(self):
        # type: () -> None
        """
        Dummy init method
        """
        _ = self
    #####################
    # To be implemented #
    #####################

    @classmethod
    def lock(cls, name, wait=None, expiration=60):
        """
        Places a mutex on the Configuration management
        To be used a context manager
        :param name: Name of the lock to acquire.
        :type name: str
        :param expiration: Expiration time of the lock (in seconds)
        :type expiration: float
        :param wait: Amount of time to wait to acquire the lock (in seconds)
        :type wait: float
        """
        return cls._passthrough(method='lock',
                                name=name,
                                wait=wait,
                                expiration=expiration)

    @classmethod
    def get_configuration_path(cls, key):
        # type: (str) -> str
        """
        Retrieve the configuration path
        For arakoon: 'arakoon://cluster_id/{0}?ini=/path/to/arakoon_cacc.ini:{0}'.format(key)
        :param key: Key to retrieve the full configuration path for
        :type key: str
        :return: Configuration path
        :rtype: str
        """
        return cls._passthrough(method='get_configuration_path',
                                key=key)

    @classmethod
    def extract_key_from_path(cls, path):
        # type: (str) -> str
        """
        Used in unittests to retrieve last key from a path
        :param path: Path to extract key from
        :type path: str
        :return: The last part of the path
        :rtype: str
        """
        return cls._passthrough(method='extract_key_from_path',
                                path=path)

    ###################
    # Implementations #
    ###################

    @classmethod
    def get(cls, key, raw=False, **kwargs):
        # type: (str, bool, **any) -> any
        """
        Get value from the configuration store
        :param key: Key to get
        :param raw: Raw data if True else json format
        :return: Value for key
        """
        # Using this bool here, because the default value itself could be None or False-ish and we want to be able to return the default value specified
        default_specified = 'default' in kwargs
        default_value = kwargs.pop('default', None)
        try:
            key_entries = key.split('|')
            data = cls._get(key_entries[0], **kwargs)
            if len(key_entries) == 1:
                return data
            try:
                temp_data = data
                for entry in key_entries[1].split('.'):
                    temp_data = temp_data[entry]
                return temp_data
            except KeyError as ex:
                raise NotFoundException(ex.message)
        except NotFoundException:
            if default_specified is True:
                return default_value
            raise

    @classmethod
    def _get(cls, key, **kwargs):
        # type: (str, **any) -> Union[dict, None]
        data = cls._passthrough(method='get',
                                key=key,
                                **kwargs)
        if key.endswith(RAW_FILES):
            return data
        return json.loads(data)

    @classmethod
    def set(cls, key, value, raw=False, transaction=None):
        # type: (str, any, bool, str) -> None
        """
        Set value in the configuration store
        :param key: Key to store
        :param value: Value to store
        :param raw: Raw data if True else apply json format
        :param transaction: Transaction to apply the delete too
        :return: None
        """
        key_entries = key.split('|')
        set_data = value
        if len(key_entries) == 1:
            cls._set(key_entries[0], set_data, transaction=transaction)
            return
        try:
            data = cls._get(key_entries[0])
        except NotFoundException:
            data = {}
        temp_config = data
        entries = key_entries[1].split('.')
        for entry in entries[:-1]:
            if entry in temp_config:
                temp_config = temp_config[entry]
            else:
                temp_config[entry] = {}
                temp_config = temp_config[entry]
        temp_config[entries[-1]] = set_data
        cls._set(key_entries[0], data, transaction=transaction)

    @classmethod
    def _set(cls, key, value, transaction=None):
        # type: (str, any, Optional[str]) -> None
        data = value
        if not key.endswith(RAW_FILES):
            data = cls._dump_data(data)
        return cls._passthrough(method='set',
                                key=key,
                                value=data,
                                transaction=transaction)

    @classmethod
    def _dump_data(cls, value):
        # type: (Union[str, Dict[Any, Any]]) -> str
        """
        Dumps data to JSON format if possible
        :param value: The value to dump
        :type value: str or dict
        :return: The converted data
        :rtype: str
        """
        try:
            data = json.loads(value)
            data = json.dumps(data, indent=4)
        except Exception:
            data = json.dumps(value, indent=4)
        return data

    @classmethod
    def delete(cls, key, remove_root=False, raw=False, transaction=None):
        # type: (str, bool, bool, str) -> None
        """
        Delete key - value from the configuration store
        :param key: Key to delete
        :param remove_root: Remove root
        :param raw: Raw data if True else apply json format
        :param transaction: Transaction to apply the delete too
        :return: None
        """
        key_entries = key.split('|')
        if len(key_entries) == 1:
            cls._delete(key_entries[0], recursive=True, transaction=transaction)
            return
        data = cls._get(key_entries[0], raw)
        temp_config = data
        entries = key_entries[1].split('.')
        if len(entries) > 1:
            for entry in entries[:-1]:
                if entry in temp_config:
                    temp_config = temp_config[entry]
                else:
                    temp_config[entry] = {}
                    temp_config = temp_config[entry]
            del temp_config[entries[-1]]
        if len(entries) == 1 and remove_root is True:
            del data[entries[0]]
        cls._set(key_entries[0], data, raw, transaction=transaction)

    @classmethod
    def _delete(cls, key, recursive, transaction=None):
        # type: (str, bool, str) -> None
        return cls._passthrough(method='delete',
                                key=key,
                                recursive=recursive,
                                transaction=transaction)

    @classmethod
    def rename(cls, key, new_key, max_retries=20):
        # type: (str, str, int) -> None
        """
        Rename path in the configuration store
        :param key: Key to store
        :type key: str
        :param new_key: New key to store
        :type new_key: str
        :param max_retries: Maximal number of attempts that can be made to store new path
        :type max_retries: int
        :return: None
        """
        return cls._passthrough(method='rename',
                                key=key,
                                new_key=new_key,
                                max_retries=max_retries)

    @classmethod
    def exists(cls, key, raw=False):
        # type: (str, bool) -> bool
        """
        Check if key exists in the configuration store
        :param key: Key to check
        :param raw: Process raw data
        :return: True if exists
        """
        try:
            cls.get(key, raw)
            return True
        except NotFoundException:
            return False

    @classmethod
    def dir_exists(cls, key):
        # type: (str) -> bool
        """
        Check if directory exists in the configuration store
        :param key: Directory to check
        :return: True if exists
        """
        return cls._passthrough(method='dir_exists', key=key)

    @classmethod
    def list(cls, key, recursive=False):
        # type: (str, bool) -> Iterable[str]
        """
        List all keys in tree in the configuration store
        :param key: Key to list
        :type key: str
        :param recursive: Recursively list all keys
        :type recursive: bool
        :return: Generator object
        """
        return cls._passthrough(method='list',
                                key=key,
                                recursive=recursive)

    @classmethod
    def begin_transaction(cls):
        # type: () -> str
        """
        Starts a new transaction. Get/set/delete calls can be chained into one
        :return: New transaction ID
        """
        return cls._passthrough(method='begin_transaction')

    @classmethod
    def apply_transaction(cls, transaction):
        # type: (str) -> None
        """
        Applies the given transaction
        :param transaction: ID of the transaction to apply
        :type transaction: str
        :return: None
        """
        return cls._passthrough(method='apply_transaction',
                                transaction=transaction)

    @classmethod
    def assert_value(cls, key, value, transaction=None, raw=False):
        # type: (str, Any, str, bool) -> None
        """
        Asserts a key-value pair
        :param key: Key to assert for
        :type key: str
        :param value: Value that the key should have
        :type value: any
        :param transaction: Transaction to apply this action too
        :type transaction: str
        :param raw: Raw data if True else apply json format
        :type raw: bool
        :return: None
        :rtype: NoneType
        """
        data = value
        # When data is None, checking for a key that does not exist. Avoids comparing None to null
        if raw is False and data is not None:
            data = cls._dump_data(data)
        return cls._passthrough(method='assert_value',
                                key=key,
                                value=data,
                                transaction=transaction)

    @classmethod
    def assert_exists(cls, key, transaction=None):
        """
        Asserts whether a given key exists
        Raises when the assertion failed
        """
        return cls._passthrough(method='assert_exists',
                                key=key,
                                transaction=transaction)

    @classmethod
    def get_client(cls):
        """
        Retrieve a configuration store client
        """
        return cls._passthrough(method='get_client')

    @classmethod
    def _passthrough(cls, method, *args, **kwargs):
        # type: (str, *any, **any) -> any
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            store = 'unittest'
        else:
            store = cls.get_store_info()
        instance = cls._clients.get(store)
        if instance is None:
            instance = cls._build_instance()
        # Map towards generic exceptions
        not_found_exception = instance.key_not_found_exception
        assertion_exception = instance.assertion_exception
        try:
            return getattr(instance, method)(*args, **kwargs)
        except not_found_exception as ex:
            # Preserve traceback
            exception_type, exception_instance, traceback = sys.exc_info()
            raise NotFoundException, NotFoundException(ex.message), traceback
        except assertion_exception as ex:
            # Preserve traceback
            exception_type, exception_instance, traceback = sys.exc_info()
            raise ConfigurationAssertionException, ConfigurationAssertionException(ex.message), traceback

    @classmethod
    def _build_instance(cls, cache=True):
        """
        Build an instance of the underlying Configuration to use
        :param cache: Cache the instance
        :type cache: bool
        :return: An instance of an underlying Configuration
        :rtype: ovs_extensions.generic.configuration.clients.base.ConfigurationBase
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            store = 'unittest'
        else:
            store = cls.get_store_info()
        if store == 'arakoon':
            from ovs_extensions.generic.configuration.clients.arakoon import ArakoonConfiguration
            instance = ArakoonConfiguration(cacc_location=cls.CACC_LOCATION)
        elif store == 'unittest':
            from ovs_extensions.generic.configuration.clients.mock_keyvalue import ConfigurationMockKeyValue
            instance = ConfigurationMockKeyValue()
        else:
            raise NotImplementedError('Store {0} is not implemented'.format(store))
        if cache is True:
            cls._clients[store] = instance
        return instance

    @classmethod
    def get_store_info(cls):
        """
        Retrieve the configuration store method. This can currently only be 'arakoon'
        :return: A tuple containing the store and params that can be passed to the configuration implementation instance
        :rtype: tuple(str, dict)
        """
        raise NotImplementedError()

    @classmethod
    def get_edition(cls):
        # type: () -> str
        """
        Retrieve the installed edition (community or enterprise)
            * Community: Free edition downloaded from apt.openvstorage.org
            * Enterprise: Paid edition which is indicated by the packages with 'ee' in their name and downloaded from apt-ee.openvstorage.com
        WARNING: This method assumes every node in the cluster has the same edition installed
        :return: The edition which has been installed
        :rtype: str
        """
        # Verify edition via configuration management
        try:
            edition = cls.get(key=cls.EDITION_KEY)
            if edition in [PackageFactory.EDITION_ENTERPRISE, PackageFactory.EDITION_COMMUNITY]:
                return edition
        except Exception:
            pass

        # Verify edition via StorageDriver package
        try:
            return PackageFactory.EDITION_ENTERPRISE if 'ee-' in check_output([PackageFactory.VERSION_CMD_SD], shell=True) else PackageFactory.EDITION_COMMUNITY
        except Exception:
            pass

        # Verify edition via ALBA package
        try:
            return PackageFactory.EDITION_ENTERPRISE if 'ee-' in check_output([PackageFactory.VERSION_CMD_ALBA], shell=True) else PackageFactory.EDITION_COMMUNITY
        except Exception:
            pass

        return PackageFactory.EDITION_COMMUNITY

    @classmethod
    def safely_store(cls, callback, max_retries=20):
        # type: (List[callable], int) -> List[Tuple[str, Any]]
        """
        Safely store a key/value pair within the persistent storage
        :param callback: Callable function which returns the key to set, current value to safe and the expected value
        When the callback resolves in an iterable different from tuple, it will iterate to set all keys at once
        :type callback: callable
        :param max_retries: Number of retries to attempt
        :type max_retries: int
        :return: List of key-value pairs of the stored items
        :rtype: list(tuple(str, any))
        :raises: ConfigurationAssertionException:
        - When the save could not happen
        """
        tries = 0
        success = False
        last_exception = None
        return_value = []
        while success is False:
            transaction = cls.begin_transaction()
            return_value = []  # Reset value
            tries += 1
            if tries > max_retries:
                raise last_exception
            callback_result = callback()
            if not isinstance(callback_result, collections.Iterable):
                raise ValueError('Callback does not produce an iterable result')
            # Multiple key/values to set
            for key, value, expected_value in callback_result:
                return_value.append((key, value))
                cls.assert_value(key, expected_value, transaction=transaction)
                cls.set(key, value, transaction=transaction)
            try:
                cls.apply_transaction(transaction)
                success = True
            except ConfigurationAssertionException as ex:
                cls._logger.warning('Asserting failed. Retrying {0} more times'.format(max_retries - tries))
                last_exception = ex
                time.sleep(randint(0, 25) / 100.0)
                cls._logger.info('Executing the passed function again')
        return return_value

    @classmethod
    def register_usage(cls, component_identifier, registration_key=None):
        # type: (str, str) -> List[str]
        """
        Registers that the component is using configuration management
        When sharing the same configuration management for multiple processes, these registrations can be used to determine
        if the configuration access can be wiped on the node
        :param component_identifier: Identifier of the component
        :type component_identifier: str
        :param registration_key: Key to register the component under
        :type registration_key: str
        :return: The currently registered users
        :rtype: List[str]
        """
        registration_key = registration_key or cls.get_registration_key()

        def _register_user_callback():
            registered_applications = cls.get(registration_key, default=None)
            new_registered_applications = (registered_applications or []) + [component_identifier]
            return [(registration_key, new_registered_applications, registered_applications)]
        return cls.safely_store(_register_user_callback, 20)[0][1]

    @classmethod
    def get_registration_key(cls):
        # type: () -> str
        """
        Generate the key to register the component under
        :return: The registration key
        :rtype: str
        """
        return cls.generate_registration_key(System.get_my_machine_id())

    @classmethod
    def generate_registration_key(cls, identifier):
        # type: (str) -> str
        """
        Generate a registration key with a given identifier
        :param identifier: Identifier for the config key
        :type identifier: str
        :return:
        """
        return COMPONENTS_KEY.format(identifier)

    @classmethod
    def unregister_usage(cls, component_identifier):
        # type: (str) -> List[str]
        """
        Registers that the component is using configuration management
        When sharing the same configuration management for multiple processes, these registrations can be used to determine
        if the configuration access can be wiped on the node
        :param component_identifier: Identifier of the component
        :type component_identifier: str
        :return: The currently registered users
        :rtype: List[str]
        """
        registration_key = cls.get_registration_key()

        def _unregister_user_callback():
            registered_applications = cls.get(registration_key, default=None)  # type: List[str]
            if not registered_applications:
                # No more entries. Save an empty list
                new_registered_applications = []
            else:
                new_registered_applications = registered_applications[:]
                if component_identifier in registered_applications:
                    new_registered_applications.remove(component_identifier)
            return [(registration_key, new_registered_applications, registered_applications)]

        return cls.safely_store(_unregister_user_callback, 20)[0][1]

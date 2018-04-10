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
import json
from subprocess import check_output
from ovs_extensions.log.logger import Logger
from ovs_extensions.packages.packagefactory import PackageFactory


class NotFoundException(Exception):
    """Not found exception."""
    pass


class ConnectionException(Exception):
    """Connection exception."""
    pass


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
    CACC_LOCATION = None
    EDITION_KEY = '{0}/edition'.format(BASE_KEY)

    _unittest_data = {}
    _clients = {}
    _logger = Logger('extensions')

    def __init__(self):
        # type: () -> None
        """
        Dummy init method
        """
        _ = self

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
        store = cls.get_store_info()
        if store == 'arakoon':
            from ovs_extensions.db.arakoon.configuration import ArakoonConfigurationLock
            return ArakoonConfigurationLock(cacc_location=cls.CACC_LOCATION, name=name, wait=wait, expiration=expiration)
        raise NotImplementedError('No lock implemented for store {0}'.format(store))

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
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return 'file://opt/OpenvStorage/config/framework.json?key={0}'.format(key)
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
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return path.split('=')[-1]
        raise NotImplementedError()

    @classmethod
    def get(cls, key, raw=False, **kwargs):
        # type: (str, bool, **kwargs) -> any
        """
        Get value from the configuration store
        :param key: Key to get
        :param raw: Raw data if True else json format
        :return: Value for key
        """
        default_specified = 'default' in kwargs  # Using this bool here, because the default value itself could be None or False-ish and we want to be able to return the default value specified
        default_value = kwargs.pop('default', None)
        try:
            key_entries = key.split('|')
            data = cls._get(key_entries[0], raw, **kwargs)
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
    def set(cls, key, value, raw=False):
        # type: (str, any, raw) -> None

        """
        Set value in the configuration store
        :param key: Key to store
        :param value: Value to store
        :param raw: Raw data if True else json format
        :return: None
        """
        key_entries = key.split('|')
        if len(key_entries) == 1:
            cls._set(key_entries[0], value, raw)
            return
        try:
            data = cls._get(key_entries[0], raw)
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
        temp_config[entries[-1]] = value
        cls._set(key_entries[0], data, raw)

    @classmethod
    def delete(cls, key, remove_root=False, raw=False):
        """
        Delete key - value from the configuration store
        :param key: Key to delete
        :param remove_root: Remove root
        :param raw: Raw data if True else json format
        :return: None
        """
        key_entries = key.split('|')
        if len(key_entries) == 1:
            cls._delete(key_entries[0], recursive=True)
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
        cls._set(key_entries[0], data, raw)

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
        cls._rename(key, new_key, max_retries)

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
        return cls._dir_exists(key)

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
        return cls._list(key, recursive=recursive)

    @classmethod
    def get_client(cls):
        """
        Retrieve a configuration store client
        """
        return cls._passthrough(method='get_client')

    @classmethod
    def _dir_exists(cls, key):
        # type: (str) -> bool
        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            stripped_key = key.strip('/')
            current_dict = cls._unittest_data
            for part in stripped_key.split('/'):
                if part not in current_dict or not isinstance(current_dict[part], dict):
                    return False
                current_dict = current_dict[part]
            return True
        # Forward call to used configuration store
        return cls._passthrough(method='dir_exists',
                                key=key)

    @classmethod
    def _list(cls, key, recursive):
        # type: (str, bool) -> Iterable(str)
        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            entries = []
            data = cls._unittest_data
            ends_with_dash = key.endswith('/')
            starts_with_dash = key.startswith('/')
            stripped_key = key.strip('/')
            for part in stripped_key.split('/'):
                if part not in data:
                    raise NotFoundException(key)
                data = data[part]
            if data:
                for sub_key in data:
                    if ends_with_dash is True:
                        entries.append('/{0}/{1}'.format(stripped_key, sub_key))
                    else:
                        entries.append(sub_key if starts_with_dash is True else '/{0}'.format(sub_key))
            elif starts_with_dash is False or ends_with_dash is True:
                entries.append('/{0}'.format(stripped_key))
            return entries
        # Forward call to used configuration store
        return cls._passthrough(method='list',
                                key=key,
                                recursive=recursive)

    @classmethod
    def _delete(cls, key, recursive):
        # type: (str, bool) -> None
        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            stripped_key = key.strip('/')
            data = cls._unittest_data
            for part in stripped_key.split('/')[:-1]:
                if part not in data:
                    raise NotFoundException(key)
                data = data[part]
            key_to_remove = stripped_key.split('/')[-1]
            if key_to_remove in data:
                del data[key_to_remove]
            return
        # Forward call to used configuration store
        return cls._passthrough(method='delete',
                                key=key,
                                recursive=recursive)

    @classmethod
    def _get(cls, key, raw, **kwargs):
        # type: (str, bool, **kwargs) -> Union[dict, None]
        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            if key in ['', '/']:
                return
            stripped_key = key.strip('/')
            data = cls._unittest_data
            for part in stripped_key.split('/')[:-1]:
                if part not in data:
                    raise NotFoundException(key)
                data = data[part]
            last_part = stripped_key.split('/')[-1]
            if last_part not in data:
                raise NotFoundException(key)
            data = data[last_part]
            if isinstance(data, dict):
                data = None
        else:
            # Forward call to used configuration store
            data = cls._passthrough(method='get',
                                    key=key,
                                    **kwargs)
        if raw is True:
            return data
        return json.loads(data)

    @classmethod
    def _set(cls, key, value, raw):
        # type: (str, any, bool) -> None
        data = value
        if raw is False:
            try:
                data = json.loads(value)
                data = json.dumps(data, indent=4)
            except Exception:
                data = json.dumps(value, indent=4)
        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            stripped_key = key.strip('/')
            ut_data = cls._unittest_data
            for part in stripped_key.split('/')[:-1]:
                if part not in ut_data:
                    ut_data[part] = {}
                ut_data = ut_data[part]

            ut_data[stripped_key.split('/')[-1]] = data
            return
        # Forward call to used configuration store
        return cls._passthrough(method='set',
                                key=key,
                                value=data)

    @classmethod
    def _rename(cls, key, new_key, max_retries):
        # type: (str, str, int) -> None
        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            stripped_key = key.strip('/')
            stripped_new_key = new_key.strip('/')
            data = cls._unittest_data
            for data_key, data_value in data.iteritems():
                if data_key.startswith(stripped_key):
                    entry_suffix = os.path.relpath(stripped_key, data_key)
                    new_path = os.path.join(stripped_new_key, entry_suffix)
                    data[new_path] = data_value
                    data.pop(data_key)
                    return
            raise NotFoundException

        # Forward call to used configuration store
        return cls._passthrough(method='rename',
                                key=key,
                                new_key=new_key,
                                max_retries=max_retries)

    @classmethod
    def _passthrough(cls, method, *args, **kwargs):
        # type: (str, *args, **kwargs) -> any
        store = cls.get_store_info()
        instance = cls._clients.get(store, cls._build_instance())
        if store == 'arakoon':
            from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonNotFound
            not_found_exception = ArakoonNotFound
        else:
            not_found_exception = Exception
        try:
            return getattr(instance, method)(*args, **kwargs)
        except not_found_exception as ex:
            raise NotFoundException(ex.message)

    @classmethod
    def _build_instance(cls, cache=True):
        """
        Build an instance of the underlying Configuration to use
        :param cache: Cache the instance
        :type cache: bool
        :return: An instance of an underlying Configuration
        :rtype: any
        """
        store = cls.get_store_info()
        if store == 'arakoon':
            from ovs_extensions.db.arakoon.configuration import ArakoonConfiguration
            instance = ArakoonConfiguration(cacc_location=cls.CACC_LOCATION)
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

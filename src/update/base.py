# Copyright (C) 2019 iNuron NV
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

import logging
from itertools import chain
from contextlib import contextmanager
from ovs_extensions.generic.filemutex import file_mutex
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.packages.packagefactory import PackageFactory as _PackageFactory
from ovs_extensions.services.interfaces.base import ServiceAbstract
from ovs_extensions.services.servicefactory import ServiceFactory as _ServiceFactory
from ovs_extensions.storage.exceptions import AssertException
from ovs_extensions.storage.persistent.pyrakoonstore import PyrakoonStore

# noinspection PyUnreachableCode
if False:
    from typing import List, Tuple


class UpdateInProgressException(EnvironmentError):
    """
    Thrown when an update couldn't start as another is already in progress
    """
    error_code = 3


class UpdateException(EnvironmentError):
    """
    Custom exception with custom error code for easy handling in the CLI
    """
    error_code = 1


class ComponentUpdater(object):
    """
    Abstract class to update a component
    Provides incompletely implemented instances of System and Configuration as they are not necessary
    The reason is to keep the code bound to extensions only so that calling the component updater isn't bound to the Framework
    This lets the updater be usable for non-framework instances (no asd-manager/no framework) after just installing the library
    """
    # Embedding both classes into the updater as they should not be used outside of this context.
    class ServiceFactory(_ServiceFactory):
        """
        Highly fiddled ServiceFactory
        """
        # Singleton holder. Does not conflict with the parent service factory
        custom_manager = None

        @classmethod
        def get_manager(cls):
            # type: () -> ServiceAbstract
            """
            Returns a service manager
            """
            if cls.custom_manager:
                return cls.custom_manager
            service_type = cls.get_service_type()
            implementation_class = cls.TYPE_IMPLEMENTATION_MAP.get(service_type)
            if implementation_class:
                return implementation_class(system=cls._get_system(),
                                            logger=cls._get_logger_instance(),
                                            configuration=cls._get_configuration(),
                                            run_file_dir=cls.RUN_FILE_DIR,
                                            monitor_prefixes=cls.MONITOR_PREFIXES,
                                            service_config_key=cls.SERVICE_CONFIG_KEY,
                                            config_template_dir=cls.CONFIG_TEMPLATE_DIR)
            raise RuntimeError('Unable to determine which service manager implementation to use')

        @classmethod
        def _get_system(cls):
            return None

        @classmethod
        def _get_configuration(cls):
            return None

    class PackageFactory(_PackageFactory):
        """
        Contains all package related information regarding OVS
        """
        @classmethod
        def _get_packages(cls):
            return {'names': ['alba', 'alba-ee', 'arakoon',
                              'openvstorage', 'openvstorage-backend', 'openvstorage-sdm',
                              'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server',
                              'volumedriver-ee-base', 'volumedriver-ee-server'],
                    'binaries': ['alba', 'alba-ee', 'arakoon',
                                 'volumedriver-no-dedup-server', 'volumedriver-ee-server']}

        @classmethod
        def _get_versions(cls):
            return {'alba': 'alba version --terse',
                    'arakoon': "arakoon --version | grep version: | awk '{print $2}'",
                    'storagedriver': "volumedriver_fs --version | grep version: | awk '{print $2}'"}

    logger = logging.getLogger(__name__)

    UPDATE_KEY_BASE = 'ovs_updates'
    PACKAGE_MANAGER = PackageFactory.get_manager()
    SERVICE_MANAGER = ServiceFactory.get_manager()
    COMPONENT = None  # type: str
    # List with tuples. [([package_name_0], binary_name, binary_location, [service_prefix_0]]
    BINARIES = None  # type: List[Tuple[List[str], str, str, List[str]]]

    @classmethod
    def get_registration_key(cls):
        """
        Build the key to register the update under
        :return: The complete key
        :rtype: str
        """
        if cls.COMPONENT is None:
            raise NotImplementedError('Unable to build a registration key for an unknown component')
        return '{}_{}'.format(cls.UPDATE_KEY_BASE, cls.COMPONENT)

    @staticmethod
    def get_persistent_client():
        # type: () -> PyrakoonStore
        """
        Retrieve a persistent client which needs
        Needs to be implemented by the callee
        """
        raise NotImplementedError()

    @classmethod
    @contextmanager
    def update_registration(cls, node_identifier):
        """
        Register the update for the node
        Falls back to a file mutex when the persistent store is not implemented
        :param node_identifier: Identifier of the node
        :type node_identifier: str
        :raises: UpdateInProgressException if an update is already in progress for this component
        """
        registration_key = cls.get_registration_key()
        try:
            persistent = cls.get_persistent_client()
            try:
                transaction = persistent.begin_transaction()
                # Simple lock based on key name
                persistent.assert_value(registration_key, None, transaction)
                persistent.set(registration_key, node_identifier, transaction)
                try:
                    persistent.apply_transaction(transaction)
                except AssertException:
                    identifier = persistent.get(registration_key)
                    raise UpdateInProgressException('An update is already in progress for component {} with identifier {}'.format(cls.COMPONENT, identifier))
                # Start code execution
                cls.logger.info("Got a hold of the lock")
                yield
            finally:
                # End lock
                transaction = persistent.begin_transaction()
                persistent.assert_value(registration_key, node_identifier, transaction)
                persistent.delete(registration_key, False, transaction)
                try:
                    persistent.apply_transaction(transaction)
                except AssertException:
                    # Something overwrote our value
                    pass
        except NotImplementedError:
            # Fallback to file mutex
            cls.logger.warning("Falling back to a file lock. No distributed lock available")
            mutex = file_mutex(registration_key)
            try:
                mutex.acquire()
                yield
            finally:
                mutex.release()

    @classmethod
    def update_binaries(cls):
        """
        Update the binary
        :return:
        """
        cls.logger.info("Starting to update all binaries")
        if cls.BINARIES is None:
            raise NotImplementedError('Unable to update packages. Binaries are not included')
        all_package_names = chain.from_iterable([b[0] for b in cls.BINARIES])
        for package_name in all_package_names:
            cls.logger.info('Updating package {}'.format(package_name))
            cls.install_package(package_name)

    @classmethod
    def install_package(cls, package):
        # type: (str) -> None
        """
        Install a package
        :param package: Package to install
        :type package: str
        :return: None
        """
        local_client = SSHClient('127.0.0.1', username='root')
        cls.PACKAGE_MANAGER.install(package, local_client)

    @classmethod
    def restart_services(cls):
        # type: () -> List[str]
        """
        Restart related services.
        :return: List of restarted services
        :rtype: List[str]
        """
        cls.logger.info("Restarting all related services")
        all_prefixes = tuple(chain.from_iterable(b[3] for b in cls.BINARIES))
        return cls.restart_services_by_prefixes(all_prefixes)

    @classmethod
    def restart_services_by_prefixes(cls, prefixes):
        # type: (Tuple[str]) -> List[str]
        """
        Restart the services that match the given prefixes
        :param prefixes: Tuple of prefixes
        :type prefixes: Tuple[str]
        :return: List of restarted services
        :rtype: List[str]
        """
        local_client = cls.get_local_root_client()
        services = cls.SERVICE_MANAGER.list_services(local_client)
        restarted_services = []
        for service in services:  # type: str
            if service.startswith(prefixes):
                try:
                    cls.SERVICE_MANAGER.restart_service(service, local_client)
                    restarted_services.append(service)
                except Exception:
                    cls.logger.warning('Failed to restart service {}'.format(service))
        return restarted_services

    @classmethod
    def do_update(cls, node_identifier):
        """
        Do the update for the volumedriver update
        :param node_identifier: Identifier of the node
        :type node_identifier: str
        """
        with cls.update_registration(node_identifier):
            cls.update_binaries()
            cls.restart_services()

    @staticmethod
    def get_local_root_client():
        # type: () -> SSHClient
        """
        Return a local root client
        :return: The root client
        :rtype: SSHClient
        """
        return SSHClient('127.0.0.1', username='root')

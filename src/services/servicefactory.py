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
Service Factory module
"""

import os
import time
from distutils.version import LooseVersion
from subprocess import check_output
from .interfaces.base import ServiceAbstract
from .interfaces.systemd import Systemd
from .interfaces.upstart import Upstart
from .mockups.systemd import SystemdMock
from ovs_extensions.log.logger import Logger


INIT = 'init'
UPSTART = 'upstart'
SYSTEMD = 'systemd'
MOCK = 'mock'


class ServiceFactory(object):
    """
    Factory class returning specialized classes
    """
    # Singleton holder
    manager = None

    RUN_FILE_DIR = None
    MONITOR_PREFIXES = None
    SERVICE_CONFIG_KEY = None
    CONFIG_TEMPLATE_DIR = None
    DEFAULT_UPDATE_ENTRY = {'packages': {},
                            'downtime': [],
                            'prerequisites': [],
                            'services_stop_start': {10: [], 20: []},   # Lowest get stopped first and started last
                            'services_post_update': {10: [], 20: []}}  # Lowest get restarted first

    _logger = Logger('extensions-service_factory')

    TYPE_IMPLEMENTATION_MAP = {UPSTART: Upstart,
                               SYSTEMD: Systemd,
                               MOCK: SystemdMock}

    @classmethod
    def get_service_type(cls):
        # type: () -> str
        """
        Gets the service manager type
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return MOCK

        init_info = check_output(['cat', '/proc/1/comm'])
        if INIT in init_info:
            version_info = check_output(['init', '--version'])
            if UPSTART in version_info:
                return UPSTART
        elif SYSTEMD in init_info:
            return SYSTEMD
        raise EnvironmentError('Unable to determine service management type')

    @classmethod
    def get_manager(cls):
        # type: () -> ServiceAbstract
        """
        Returns a service manager
        """
        if cls.manager is None:
            service_type = cls.get_service_type()
            implementation_class = cls.TYPE_IMPLEMENTATION_MAP.get(service_type)
            if implementation_class:
                cls.manager = implementation_class(system=cls._get_system(),
                                                   logger=cls._get_logger_instance(),
                                                   configuration=cls._get_configuration(),
                                                   run_file_dir=cls.RUN_FILE_DIR,
                                                   monitor_prefixes=cls.MONITOR_PREFIXES,
                                                   service_config_key=cls.SERVICE_CONFIG_KEY,
                                                   config_template_dir=cls.CONFIG_TEMPLATE_DIR)

        if cls.manager is None:
            raise RuntimeError('Unknown ServiceManager')
        return cls.manager

    @classmethod
    def _get_system(cls):
        raise NotImplementedError()

    @classmethod
    def _get_configuration(cls):
        raise NotImplementedError()

    @classmethod
    def _get_logger_instance(cls):
        return Logger('extensions-services')

    @classmethod
    def change_service_state(cls, client, name, state, logger=None):
        """
        Starts/stops/restarts a service
        :param client: SSHClient on which to connect and change service state
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param name: Name of the service
        :type name: str
        :param state: State to put the service in
        :type state: str
        :param logger: Logger Object
        :type logger: ovs_extensions.log.logger.Logger
        :return: None
        :rtype: NoneType
        """
        service_manager = cls.get_manager()
        action = None
        status = service_manager.get_service_status(name, client=client)
        if status != 'active' and state in ['start', 'restart']:
            if logger is not None:
                logger.info('{0}: Starting service {1}'.format(client.ip, name))
            service_manager.start_service(name, client=client)
            action = 'Started'
        elif status == 'active' and state == 'stop':
            if logger is not None:
                logger.info('{0}: Stopping service {1}'.format(client.ip, name))
            service_manager.stop_service(name, client=client)
            action = 'Stopped'
        elif status == 'active' and state == 'restart':
            if logger is not None:
                logger.info('{0}: Restarting service {1}'.format(client.ip, name))
            service_manager.restart_service(name, client=client)
            action = 'Restarted'

        if action is None:
            print '  [{0}] {1} already {2}'.format(client.ip, name, 'running' if status == 'active' else 'halted')
        else:
            if logger is not None:
                logger.info('{0}: {1} service {2}'.format(client.ip, action, name))
            print '  [{0}] {1} {2}'.format(client.ip, name, action.lower())

    @classmethod
    def wait_for_service(cls, client, name, status, logger, wait=10):
        """
        Wait for service to enter status
        :param client: SSHClient to run commands
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param name: Name of service
        :type name: str
        :param status: 'active' if running, 'inactive' if halted
        :type status: str
        :param logger: Logger object
        :type logger: ovs_extensions.log.logger.Logger
        :param wait: Time to wait for the service to enter the specified state
        :type wait: int
        :return: None
        :rtype: NoneType
        """
        max_wait = 10 if wait <= 10 else wait
        service_manager = cls.get_manager()
        service_status = service_manager.get_service_status(name, client)
        while wait > 0:
            if service_status == status:
                return
            logger.debug('... waiting for service {0}'.format(name))
            wait -= 1
            time.sleep(max_wait - wait)
            service_status = service_manager.get_service_status(name, client)
        raise RuntimeError('Service {0} does not have expected status: Expected: {1} - Actual: {2}'.format(name, status, service_status))

    @classmethod
    def get_service_update_versions(cls, client, service_name, binary_versions, package_name=None):
        """
        Validate whether the service requires a restart, based upon the currently installed binary version
        :param client: Client on which to execute the validation
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param service_name: Name of the service to check
        :type service_name: str
        :param binary_versions: Mapping between the package_names and their available binary version. E.g.: {'arakoon': 1.9.22}
        :type binary_versions: dict
        :param package_name: Name of the package to match for in the service run file (Only applicable if the service depends on multiple packages)
        :type package_name: str
        :return: The services which require a restart
        :rtype: dict
        """
        version_file = '{0}/{1}.version'.format(cls.RUN_FILE_DIR, service_name)
        if not client.file_exists(version_file):
            ServiceFactory._logger.error('No service file found for service {0} in {1} on node with IP {2}'.format(service_name, cls.RUN_FILE_DIR, client.ip))
            return

        # Verify whether a restart is required based on the content of the file and binary_versions passed
        for version in client.file_read(version_file).strip().split(';'):
            if not version:
                continue
            pkg_name = version.strip().split('=')[0]
            running_version = version.strip().split('=')[1]
            if (package_name is None or pkg_name == package_name) and running_version:
                if LooseVersion(running_version) < binary_versions[pkg_name]:
                    return {'installed': running_version,
                            'candidate': str(binary_versions[pkg_name])}
                if '-reboot' in running_version:
                    return {'installed': 'service_restart',
                            'candidate': str(binary_versions[pkg_name])}

    @classmethod
    def remove_services_marked_for_removal(cls, client, package_names):
        """
        During update we potentially mark services for removal because they have been updated, replaced, became obsolete, ...
        These services linked to the packages which have been updated need to be removed
        :param client: SSHClient on which to remove the services
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: The packages which have been updated and thus need to be checked
        :type package_names: set[str]
        :return: None
        :rtype: NoneType
        """
        for version_file in client.file_list(directory=cls.RUN_FILE_DIR):
            if not version_file.endswith('.remove'):
                continue

            service_manager = cls.get_manager()
            file_name = '{0}/{1}'.format(cls.RUN_FILE_DIR, version_file)
            contents = client.file_read(filename=file_name)
            for part in contents.split(';'):
                if part.split('=')[0] in package_names:
                    service_name = version_file.replace('.remove', '').replace('.version', '')
                    cls._logger.warning('{0}: Removing service {1}'.format(client.ip, service_name))
                    service_manager.stop_service(name=service_name, client=client)
                    service_manager.remove_service(name=service_name, client=client)
                    client.file_delete(filenames=[file_name])
                    break

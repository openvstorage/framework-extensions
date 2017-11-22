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
import copy
import time
from distutils.version import LooseVersion
from subprocess import check_output
from ovs_extensions.log.logger import Logger
from ovs_extensions.services.interfaces.systemd import Systemd
from ovs_extensions.services.interfaces.upstart import Upstart
from ovs_extensions.services.mockups.systemd import SystemdMock


class ServiceFactory(object):
    """
    Factory class returning specialized classes
    """
    RUN_FILE_DIR = None
    MONITOR_PREFIXES = None
    SERVICE_CONFIG_KEY = None
    CONFIG_TEMPLATE_DIR = None

    _logger = Logger('extensions-service_factory')

    @classmethod
    def get_service_type(cls):
        """
        Gets the service manager type
        """
        init_info = check_output('cat /proc/1/comm', shell=True)
        if 'init' in init_info:
            version_info = check_output('init --version', shell=True)
            if 'upstart' in version_info:
                return 'upstart'
        elif 'systemd' in init_info:
            return 'systemd'
        return None

    @classmethod
    def get_manager(cls):
        """
        Returns a service manager
        """
        if not hasattr(cls, 'manager') or cls.manager is None:
            implementation_class = None
            if os.environ.get('RUNNING_UNITTESTS') == 'True':
                implementation_class = SystemdMock
            else:
                service_type = cls.get_service_type()
                if service_type == 'upstart':
                    implementation_class = Upstart
                elif service_type == 'systemd':
                    implementation_class = Systemd
            if implementation_class is not None:
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
        raise NotImplementedError()

    @classmethod
    def get_services_with_version_files(cls, storagerouter):
        raise NotImplementedError()

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
    def get_services_to_update(cls, client, service_info, binaries):
        """
        Validate whether the services passed require a restart
        The services being checked here are services which rely on binaries such as alba, arakoon, volumedriver_fs ...
        Therefore we pass in the available binary versions to be able to compare with the currently running version
        :param client: Client on which to execute the validation
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param service_info: Information about the services {<component>: {<package_name>: {10: [<service_name>, ...] } } }
        :type service_info: dict
        :param binaries: Mapping between the package_names and their available binary version. E.g.: {'arakoon': 1.9.22}
        :type binaries: dict
        :return: The services which require a restart
        :rtype: dict
        """
        default_entry = {'candidate': None,
                         'installed': None,
                         'services_to_restart': {}}
        update_info = {}
        for component, all_info in service_info.iteritems():
            for package_name, info in all_info.iteritems():
                for importance, services in info.iteritems():
                    for service in services:
                        version_file = '{0}/{1}.version'.format(cls.RUN_FILE_DIR, service)
                        if not client.file_exists(version_file):
                            # The .version file was not found, so we don't know whether to restart it or not. Let's choose the safest option
                            if component not in update_info:
                                update_info[component] = {}
                            if package_name not in update_info[component]:
                                update_info[component][package_name] = copy.deepcopy(default_entry)
                            if importance not in update_info[component][package_name]['services_to_restart']:
                                update_info[component][package_name]['services_to_restart'][importance] = []
                            update_info[component][package_name]['installed'] = '{0}-reboot'.format(binaries[package_name])
                            update_info[component][package_name]['candidate'] = str(binaries[package_name])
                            update_info[component][package_name]['services_to_restart'][importance].append(service)
                            continue

                        # The .version file exists. Base restart requirement on its content
                        for version in client.file_read(version_file).strip().split(';'):
                            version = version.strip()
                            running_version = None
                            if '=' in version:
                                package_name = version.split('=')[0]
                                running_version = version.split('=')[1]
                            elif version:
                                running_version = version

                            if running_version is not None and (LooseVersion(running_version) < binaries[package_name] or '-reboot' in running_version):
                                if component not in update_info:
                                    update_info[component] = {}
                                if package_name not in update_info[component]:
                                    update_info[component][package_name] = copy.deepcopy(default_entry)
                                if importance not in update_info[component][package_name]['services_to_restart']:
                                    update_info[component][package_name]['services_to_restart'][importance] = []
                                update_info[component][package_name]['installed'] = running_version
                                update_info[component][package_name]['candidate'] = str(binaries[package_name])
                                update_info[component][package_name]['services_to_restart'][importance].append(service)
        return update_info

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

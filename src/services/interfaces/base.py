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

"""
Service interface
"""

import os
import re
from abc import ABCMeta, abstractmethod
from ConfigParser import ConfigParser
from ovs_extensions.generic.configuration import Configuration
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs_extensions.log.logger import Logger

# Helps resolves in the IDE without enforcing the dependency
# noinspection PyUnreachableCode
if False:
    from typing import List, Optional, Generator, Tuple


class ServiceAbstract(object):
    """
    Contains all logic related to non-implementation bound services
    - Registration of services within config management
    - Signaling and parsing `ps`
    """

    __metaclass__ = ABCMeta

    SERVICE_DIR = None
    SERVICE_SUFFIX = None
    SYSTEM_SERVICE_DIR = None
    OVS_SERVICE_PREFIX = 'ovs-'

    def __init__(self, system, configuration, run_file_dir, monitor_prefixes, service_config_key, config_template_dir, logger):
        # type: (System, Configuration, str, List[str], str, str, Logger) -> None
        """
        Instantiate a service manager
        :param system: System implementation
        :type system: System
        :param configuration: Configuration implementation
        :type configuration: Configuration
        :param run_file_dir: Directory path where all .run files have to be created
        :type run_file_dir: str
        :param monitor_prefixes: Service name prefixes to pick up on when monitoring the services
        :type monitor_prefixes: List[str]
        :param service_config_key: Path within configuration to add the service to with 2 open format.
        eg: /something/{<machine id. Added by code>}/else/{<service name. Added by code>}.
        First format is the identifier of the machine. Second format is the cleaned service name
        :type service_config_key: str
        :param config_template_dir: Directory path to find all service templates in
        :type config_template_dir: str
        :param logger: Logger instance to use
        :type logger: Logger
        """
        self._logger = logger
        self._system = system
        self._run_file_dir = run_file_dir
        self._configuration = configuration
        self._monitor_prefixes = monitor_prefixes
        self.service_config_key = service_config_key
        self._config_template_dir = config_template_dir

    @classmethod
    def _service_exists(cls, name, client, path):
        # type: (str, SSHClient, str) -> bool
        """
        Determine if the service exists
        :param name: Name of the service file
        :type name: str
        :param client: Client to use
        :type client: SSHClient
        :param path: Path to service file
        :type path: str
        :return: True if the service exists else False
        :rtype: bool
        """
        # Cleaning for safety. os.path.join('something', '/') ends up with '/'
        name = name.rstrip('/')
        path = cls.SERVICE_DIR if path is None else path.rstrip('/')
        file_to_check = cls.get_service_file_path(name, path)
        return client.file_exists(file_to_check)

    def _get_name(self, name, client, path=None, log=True):
        # type: (str, SSHClient, Optional[str], Optional[bool]) -> str
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        :param name: Name of the service
        :type name: str
        :param client: Client to use
        :type client: SSHClient
        :param path: Path to service file
        :type path: str
        :param log: Log if the service could not be found
        :type log: bool
        :return: Full service name
        :rtype: str
        :raises: ValueError if the service could not be found
        """
        if self.SYSTEM_SERVICE_DIR is None:
            raise NotImplementedError()
        for potential_name, potential_path in [(name, path),  # Non system
                                               (name, self.SYSTEM_SERVICE_DIR),  # System
                                               (self.OVS_SERVICE_PREFIX + name, path)]:  # Ovs
            if self._service_exists(potential_name, client, potential_path):
                return potential_name
        if log is True:
            self._logger.info('Service {0} could not be found.'.format(name))
        raise ValueError('Service {0} could not be found.'.format(name))

    @classmethod
    def list_services(cls, client):
        # type: (SSHClient) -> Generator[str]
        """
        List all created services on a system
        :param client: Client on which to list all the services
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: List of all services which have been created at some point
        :rtype: generator
        """
        raise NotImplementedError()

    @abstractmethod
    def add_service(self, name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False):
        # type: (str, SSHClient, Optional[dict], Optional[str], Optional[str], Optional[bool]) -> dict
        """
        Add a service
        :param name: Template name of the service to add
        :type name: str
        :param client: Client on which to add the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param params: Additional information about the service
        :type params: dict or None
        :param target_name: Overrule default name of the service with this name
        :type target_name: str or None
        :param startup_dependency: Additional startup dependency
        :type startup_dependency: str or None
        :param delay_registration: Register the service parameters in the config management right away or not
        :type delay_registration: bool
        :return: Parameters used by the service
        :rtype: dict
        """

    def regenerate_service(self, name, client, target_name):
        # type: (str, SSHClient, str) -> None
        """
        Regenerates the service files of a service.
        :param name: Template name of the service to regenerate
        :type name: str
        :param client: Client on which to regenerate the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param target_name: The current service name eg ovs-volumedriver_flash01.service
        :type target_name: str
        :return: None
        :rtype: NoneType
        :raises: RuntimeError if the regeneration failed
        """
        configuration_key = self.service_config_key.format(self._system.get_my_machine_id(client),
                                                           ExtensionsToolbox.remove_prefix(target_name, self.OVS_SERVICE_PREFIX))
        # If the entry is stored in arakoon, it means the service file was previously made
        if not self._configuration.exists(configuration_key):
            raise RuntimeError('Service {0} was not previously added and cannot be regenerated.'.format(target_name))
        # Rewrite the service file
        service_params = self._configuration.get(configuration_key)
        startup_dependency = service_params['STARTUP_DEPENDENCY']
        if startup_dependency == '':
            startup_dependency = None
        else:
            startup_dependency = '.'.join(startup_dependency.split('.')[:-1])  # Remove .service from startup dependency
        output = self.add_service(name=name,
                                  client=client,
                                  params=service_params,
                                  target_name=target_name,
                                  startup_dependency=startup_dependency,
                                  delay_registration=True)
        if output is None:
            raise RuntimeError('Regenerating files for service {0} has failed'.format(target_name))

    @abstractmethod
    def get_service_status(self, name, client):
        # type: (str, SSHClient) -> str
        """
        Retrieve the status of a service
        :param name: Name of the service to retrieve the status of
        :type name: str
        :param client: Client on which to retrieve the status
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The status of the service
        :rtype: str
        """

    def remove_service(self, name, client, delay_unregistration=False):
        # type: (str, SSHClient, Optional[bool]) -> None
        """
        Remove a service
        :param name: Name of the service to remove
        :type name: str
        :param client: Client on which to remove the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param delay_unregistration: Un-register the service parameters in the config management right away or not
        :type delay_unregistration: bool
        :return: None
        :rtype: NoneType
        """
        name = self._get_name(name, client)
        run_file_path = self.get_run_file_path(name)
        if client.file_exists(run_file_path):
            client.file_delete(run_file_path)
        service_path = self.get_service_file_path(name)
        client.file_delete(service_path)

        if delay_unregistration is False:
            self.unregister_service(service_name=name, node_name=self._system.get_my_machine_id(client))

    @abstractmethod
    def start_service(self, name, client, timeout=5):
        # type: (str, SSHClient, Optional[int]) -> None
        """
        Start a service
        :param name: Name of the service to start
        :type name: str
        :param client: Client on which to start the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param timeout: Timeout within to verify the service status (in seconds)
        :type timeout: int
        :return: None
        :rtype: NoneType
        """

    @abstractmethod
    def stop_service(self, name, client, timeout=5):
        # type: (str, SSHClient, Optional[int]) -> None
        """
        Stop a service
        :param name: Name of the service to stop
        :type name: str
        :param client: Client on which to stop the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param timeout: Timeout within to verify the service status (in seconds)
        :type timeout: int
        :return: None
        :rtype: NoneType
        :raises: RuntimeError if the service faile to stop
        """

    @abstractmethod
    def restart_service(self, name, client, timeout=5):
        # type: (str, SSHClient, Optional[int]) -> None
        """
        Restart a service
        :param name: Name of the service to restart
        :type name: str
        :param client: Client on which to restart the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param timeout: Timeout within to verify the service status (in seconds)
        :type timeout: int
        :return: None
        :rtype: NoneType
        :raises: RuntimeError: if the service could not be restarted
        """

    def has_service(self, name, client):
        # type: (str, SSHClient) -> bool
        """
        Verify existence of a service
        :param name: Name of the service to verify
        :type name: str
        :param client: Client on which to check for the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: Whether the service exists
        :rtype: bool
        """
        try:
            self._get_name(name, client, log=False)
        except ValueError:
            return False
        return True

    @abstractmethod
    def get_service_pid(self, name, client):
        # type: (str, SSHClient) -> int
        """
        Retrieve the PID of a service
        :param name: Name of the service to retrieve the PID for
        :type name: str
        :param client: Client on which to retrieve the PID for the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The PID of the service or 0 if no PID found
        :rtype: int
        """

    def send_signal(self, name, signal, client):
        # type: (str, int, SSHClient) -> None
        """
        Send a signal to a service
        :param name: Name of the service to send a signal
        :type name: str
        :param signal: Signal to pass on to the service
        :type signal: int
        :param client: Client on which to send a signal to the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        name = self._get_name(name, client)
        pid = self.get_service_pid(name, client)
        if pid == 0:
            raise RuntimeError('Could not determine PID to send signal to')
        client.run(['kill', '-s', signal, pid])

    @abstractmethod
    def monitor_services(self):
        # type: () -> None
        """
        Monitor the local services
        :return: None
        :rtype: NoneType
        """

    def register_service(self, node_name, service_metadata):
        # type: (str, dict) -> None
        """
        Register the metadata of the service to the configuration management
        :param node_name: Name of the node on which the service is running
        :type node_name: str
        :param service_metadata: Metadata of the service
        :type service_metadata: dict
        :return: None
        :rtype: NoneType
        """
        service_name = service_metadata['SERVICE_NAME']
        self._configuration.set(key=self.service_config_key.format(node_name, ExtensionsToolbox.remove_prefix(service_name, self.OVS_SERVICE_PREFIX)),
                                value=service_metadata)

    def unregister_service(self, node_name, service_name):
        # type: (str, str) -> None
        """
        Un-register the metadata of a service from the configuration management
        :param node_name: Name of the node on which to un-register the service
        :type node_name: str
        :param service_name: Name of the service to clean from the configuration management
        :type service_name: str
        :return: None
        :rtype: NoneType
        """
        self._configuration.delete(key=self.service_config_key.format(node_name, ExtensionsToolbox.remove_prefix(service_name, self.OVS_SERVICE_PREFIX)))

    def is_rabbitmq_running(self, client):
        # type: (SSHClient) -> Tuple[int, bool]
        """
        Check if rabbitmq is correctly running
        :param client: Client on which to check the rabbitmq process
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The PID of the process and a bool indicating everything runs as expected
        :rtype: tuple
        """
        rabbitmq_running = False
        rabbitmq_pid_ctl = -1
        rabbitmq_pid_sm = -1
        output = client.run(['rabbitmqctl', 'status'], allow_nonzero=True)
        if output:
            match = re.search('\{pid,(?P<pid>\d+?)\}', output)
            if match is not None:
                match_groups = match.groupdict()
                if 'pid' in match_groups:
                    rabbitmq_running = True
                    rabbitmq_pid_ctl = match_groups['pid']

        if self.has_service('rabbitmq-server', client) and self.get_service_status('rabbitmq-server', client) == 'active':
            rabbitmq_running = True
            rabbitmq_pid_sm = self.get_service_pid('rabbitmq-server', client)

        same_process = rabbitmq_pid_ctl == rabbitmq_pid_sm
        self._logger.debug('Rabbitmq is reported {0}running, pids: {1} and {2}'.format('' if rabbitmq_running else 'not ',
                                                                                       rabbitmq_pid_ctl,
                                                                                       rabbitmq_pid_sm))
        return rabbitmq_running, same_process

    def extract_from_service_file(self, name, client, entries=None):
        # type: (str, SSHClient, List[str]) -> List[str]
        """
        Extract an entry, multiple entries or the entire service file content for a service
        :param name: Name of the service
        :type name: str
        :param client: Client on which to extract something from the service file
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param entries: Entries to extract
        :type entries: list
        :return: The requested entry information or entire service file content if entry=None
        :rtype: list
        """
        if self.has_service(name=name, client=client) is False:
            return []

        try:
            name = self._get_name(name=name, client=client)
            contents = client.file_read(self.get_service_file_path(name)).splitlines()
        except Exception:
            self._logger.exception('Failure to retrieve contents for service {0} on node with IP {1}'.format(name, client.ip))
            return []

        if entries is None:
            return contents

        return_value = []
        for line in contents:
            for entry in entries:
                if entry in line:
                    return_value.append(line)
        return return_value

    def get_service_start_time(self, name, client):
        # type: (str, SSHClient) -> str
        """
        Retrieves the start time of the service
        :param name: Name of the service to retrieve the PID for
        :type name: str
        :param client: Client on which to retrieve the PID for the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :raises ValueError when no PID could be found for the given process
        :return: A string representing the datetime of when the service was started eg Mon Jan 1 3:30:00 2018
        :rtype: str
        """
        pid = self.get_service_pid(name, client)
        if pid in [0, -1]:
            raise ValueError('No PID could be found for service {0} on node with IP {1}'.format(name, client.ip))
        return client.run(['ps', '-o', 'lstart', '-p', pid]).strip().splitlines()[-1]

    @classmethod
    def get_service_file_path(cls, name, path=None):
        # type: (str, str) -> str
        """
        Get the path to a service
        :param name: Name of the service
        :type name: str
        :param path: Path to service file
        :type path: str
        :return: The path to the service file
        :rtype: str
        """
        if any(v is None for v in [cls.SERVICE_DIR, cls.SERVICE_SUFFIX]):
            raise NotImplementedError()
        path = path or cls.SERVICE_DIR
        return os.path.join(path, name + cls.SERVICE_SUFFIX)

    def get_run_file_path(self, name):
        # type: (str) -> str
        """
        Get the path to the run file for the given service
        This is tied to the template files as they specify something like `/opt/OpenvStorage/run/<SERVICE_NAME>.version`
        :param name: Name of the service
        :type name: str
        :return: Path to the file
        :rtype: str
        """
        non_ovs_name = ExtensionsToolbox.remove_prefix(name, self.OVS_SERVICE_PREFIX)
        return os.path.join(self._run_file_dir, non_ovs_name, '.version')

    @staticmethod
    def get_config_parser():
        # type: () -> ConfigParser
        """
        Retrieve the config parser for the implementation type
        :return: A config parser instance
        :rtype: ConfigParser
        """
        raise NotImplementedError()

    def get_service_fd(self, name, client):
        # type: (str, SSHClient) -> str
        """
        Returns the open file descriptors for a service that is running
        :param name: name of the service
        :type name: str
        :param client: Client on which to extract something from the service file
        :type client: ovs_extensions.generic.sshclient.SSHClient
        """
        pid = self.get_service_pid(name, client)
        file_descriptors = client.run(['lsof',  '-i', '-a', '-p', pid])
        return file_descriptors.split('\n')[1:]


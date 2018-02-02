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
Systemd Mock module
"""
import random
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class SystemdMock(object):
    """
    Contains all logic related to Systemd Mock services
    """
    services = {}

    def __init__(self, system, configuration, run_file_dir, monitor_prefixes, service_config_key, config_template_dir):
        """
        Init method
        """
        self._system = system
        self._run_file_dir = run_file_dir
        self._configuration = configuration
        self._monitor_prefixes = monitor_prefixes
        self.service_config_key = service_config_key
        self._config_template_dir = config_template_dir

    def add_service(self, name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False):
        """
        Adds a mocked service
        """
        if params is None:
            params = {}

        key = 'None' if client is None else client.ip
        name = name if target_name is None else target_name
        params.update({'SERVICE_NAME': ExtensionsToolbox.remove_prefix(name, 'ovs-'),
                       'STARTUP_DEPENDENCY': '' if startup_dependency is None else '{0}.service'.format(startup_dependency)})
        if self.has_service(name=name, client=client) is False:
            SystemdMock.services[key] = {name: {'state': 'HALTED',
                                                'pid': None}}
        if delay_registration is False:
            self.register_service(node_name=self._system.get_my_machine_id(client), service_metadata=params)
        return params

    def get_service_status(self, name, client):
        """
        Retrieve the mocked service status
        """
        name = self._get_name(name, client)
        key = 'None' if client is None else client.ip
        if SystemdMock.services.get(key, {}).get(name, {}).get('state') == 'RUNNING':
            return 'active'
        return 'inactive'

    def remove_service(self, name, client, delay_unregistration=False):
        """
        Remove a mocked service
        """
        name = self._get_name(name, client)
        key = 'None' if client is None else client.ip
        if name in SystemdMock.services[key]:
            SystemdMock.services[key].pop(name)
        if delay_unregistration is False:
            self.unregister_service(service_name=name, node_name=self._system.get_my_machine_id(client))

    def start_service(self, name, client):
        """
        Start a mocked service
        """
        name = self._get_name(name, client)
        key = 'None' if client is None else client.ip
        if name not in SystemdMock.services[key]:
            raise RuntimeError('Service {0} does not exist'.format(name))
        SystemdMock.services[key][name]['state'] = 'RUNNING'
        self.get_service_pid(name, client)  # Add a PID
        if self.get_service_status(name, client) != 'active':
            raise RuntimeError('Start {0} failed'.format(name))

    def stop_service(self, name, client):
        """
        Stop a mocked service
        """
        name = self._get_name(name, client)
        key = 'None' if client is None else client.ip
        if name not in SystemdMock.services[key]:
            raise RuntimeError('Service {0} does not exist'.format(name))
        SystemdMock.services[key][name]['state'] = 'HALTED'
        if self.get_service_status(name, client) != 'inactive':
            raise RuntimeError('Stop {0} failed'.format(name))

    def restart_service(self, name, client):
        """
        Restart a mocked service
        """
        name = self._get_name(name, client)
        self.stop_service(name, client)
        self.start_service(name, client)

    def has_service(self, name, client):
        """
        Verify whether a mocked service exists
        """
        try:
            name = self._get_name(name, client)
            key = 'None' if client is None else client.ip
            return name in SystemdMock.services[key]
        except ValueError:
            return False

    def register_service(self, node_name, service_metadata):
        """
        Register the metadata of the service to the configuration management
        :param node_name: Name of the node on which the service is running
        :type node_name: str
        :param service_metadata: Metadata of the service
        :type service_metadata: dict
        :return: None
        """
        service_name = service_metadata['SERVICE_NAME']
        self._configuration.set(key=self.service_config_key.format(node_name, ExtensionsToolbox.remove_prefix(service_name, 'ovs-')),
                                value=service_metadata)

    def unregister_service(self, node_name, service_name):
        """
        Un-register the metadata of a service from the configuration management
        :param node_name: Name of the node on which to un-register the service
        :type node_name: str
        :param service_name: Name of the service to clean from the configuration management
        :type service_name: str
        :return: None
        """
        self._configuration.delete(key=self.service_config_key.format(node_name, ExtensionsToolbox.remove_prefix(service_name, 'ovs-')))

    def get_service_pid(self, name, client):
        """
        Retrieve the PID of a mocked service
        This will generate a PID and store it for further mocked use
        :param name: Name of the service to retrieve the PID for
        :type name: str
        :param client: Client on which to retrieve the PID for the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The PID of the service or 0 if no PID found
        :rtype: int
        """
        pid = 0
        name = self._get_name(name, client)
        key = 'None' if client is None else client.ip
        if self.get_service_status(name, client) == 'active':
            pid = SystemdMock.services[key][name].get('pid')
            if pid is None:
                pid = random.randint(1000, 65535)
                SystemdMock.services[key][name]['pid'] = pid
        return pid

    @classmethod
    def extract_from_service_file(cls, name, client, entries=None):
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
        _ = name, client, entries
        return []

    @classmethod
    def _service_exists(cls, name, client, path):
        """
        Verify whether a mocked service exists
        """
        _ = path
        key = 'None' if client is None else client.ip
        return name in SystemdMock.services.get(key, {})

    def _get_name(self, name, client, path=None, log=True):
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        """
        _ = log
        if self._service_exists(name, client, path):
            return name
        if self._service_exists(name, client, '/lib/systemd/system/'):
            return name
        name = 'ovs-{0}'.format(name)
        if self._service_exists(name, client, path):
            return name
        raise ValueError('Service {0} could not be found.'.format(name))

    @classmethod
    def _clean(cls):
        """
        Clean up mocked Class
        """
        SystemdMock.services = {}

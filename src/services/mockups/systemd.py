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

from ovs_extensions.generic.configuration import Configuration
from ovs_extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class SystemdMock(object):
    """
    Contains all logic related to Systemd Mock services
    """
    SERVICE_CONFIG_KEY = '/ovs/framework/hosts/{0}/services/{1}'
    services = {}

    @classmethod
    def add_service(cls, name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False):
        """
        Adds a mocked service
        """
        if params is None:
            params = {}

        key = 'None' if client is None else client.ip
        name = name if target_name is None else target_name
        params.update({'SERVICE_NAME': ExtensionsToolbox.remove_prefix(name, 'ovs-'),
                       'STARTUP_DEPENDENCY': '' if startup_dependency is None else '{0}.service'.format(startup_dependency)})
        if cls.has_service(name=name, client=client) is False:
            cls.services[key] = {name: 'HALTED'}
        if delay_registration is False:
            cls.register_service(node_name=System.get_my_machine_id(client), service_metadata=params)
        return params

    @classmethod
    def get_service_status(cls, name, client):
        """
        Retrieve the mocked service status
        """
        name = cls._get_name(name, client)
        key = 'None' if client is None else client.ip
        if cls.services.get(key, {}).get(name) == 'RUNNING':
            return 'active'
        return 'inactive'

    @classmethod
    def remove_service(cls, name, client, delay_unregistration=False):
        """
        Remove a mocked service
        """
        name = cls._get_name(name, client)
        key = 'None' if client is None else client.ip
        if name in cls.services[key]:
            cls.services[key].pop(name)
        if delay_unregistration is False:
            cls.unregister_service(service_name=name, node_name=System.get_my_machine_id(client))

    @classmethod
    def start_service(cls, name, client):
        """
        Start a mocked service
        """
        name = cls._get_name(name, client)
        key = 'None' if client is None else client.ip
        if name not in cls.services[key]:
            raise RuntimeError('Service {0} does not exist'.format(name))
        cls.services[key][name] = 'RUNNING'
        if cls.get_service_status(name, client) != 'active':
            raise RuntimeError('Start {0} failed'.format(name))

    @classmethod
    def stop_service(cls, name, client):
        """
        Stop a mocked service
        """
        name = cls._get_name(name, client)
        key = 'None' if client is None else client.ip
        if name not in cls.services[key]:
            raise RuntimeError('Service {0} does not exist'.format(name))
        cls.services[key][name] = 'HALTED'
        if cls.get_service_status(name, client) != 'inactive':
            raise RuntimeError('Stop {0} failed'.format(name))

    @classmethod
    def restart_service(cls, name, client):
        """
        Restart a mocked service
        """
        name = cls._get_name(name, client)
        cls.stop_service(name, client)
        cls.start_service(name, client)

    @classmethod
    def has_service(cls, name, client):
        """
        Verify whether a mocked service exists
        """
        try:
            name = cls._get_name(name, client)
            key = 'None' if client is None else client.ip
            return name in cls.services[key]
        except ValueError:
            return False

    @classmethod
    def register_service(cls, node_name, service_metadata):
        """
        Register the metadata of the service to the configuration management
        :param node_name: Name of the node on which the service is running
        :type node_name: str
        :param service_metadata: Metadata of the service
        :type service_metadata: dict
        :return: None
        """
        service_name = service_metadata['SERVICE_NAME']
        Configuration.set(key=cls.SERVICE_CONFIG_KEY.format(node_name, ExtensionsToolbox.remove_prefix(service_name, 'ovs-')),
                          value=service_metadata)

    @classmethod
    def unregister_service(cls, node_name, service_name):
        """
        Un-register the metadata of a service from the configuration management
        :param node_name: Name of the node on which to un-register the service
        :type node_name: str
        :param service_name: Name of the service to clean from the configuration management
        :type service_name: str
        :return: None
        """
        Configuration.delete(key=cls.SERVICE_CONFIG_KEY.format(node_name, ExtensionsToolbox.remove_prefix(service_name, 'ovs-')))

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
        return name in cls.services.get(key, {})

    @classmethod
    def _get_name(cls, name, client, path=None, log=True):
        """
        Make sure that for e.g. 'ovs-workers' the given service name can be either 'ovs-workers' as just 'workers'
        """
        _ = log
        if cls._service_exists(name, client, path):
            return name
        if cls._service_exists(name, client, '/lib/systemd/system/'):
            return name
        name = 'ovs-{0}'.format(name)
        if cls._service_exists(name, client, path):
            return name
        raise ValueError('Service {0} could not be found.'.format(name))

    @classmethod
    def _clean(cls):
        """
        Clean up mocked Class
        """
        cls.services = {}

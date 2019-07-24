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
Upstart module
"""

import os
import re
import time
from ConfigParser import ConfigParser
from subprocess import CalledProcessError, check_output
from .base import ServiceAbstract
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class Upstart(ServiceAbstract):
    """
    Contains all logic related to Upstart services
    """
    SERVICE_DIR = os.path.join(os.path.sep, 'etc', 'init')
    SERVICE_SUFFIX = '.conf'
    # Contains the Systems services. Differ from /etc/init
    SYSTEM_SERVICE_DIR = os.path.join(os.path.sep, 'etc', 'init.d')

    def add_service(self, name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False, path=None):
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
        :param path: path to add the service to
        :type path: str
        :return: Parameters used by the service
        :rtype: dict
        """
        if params is None:
            params = {}
        if path is None:
            path = self._config_template_dir.format('upstart')
        else:
            path = path.format('upstart')
        service_name = self._get_name(name, client, path)

        template_file = '{0}/{1}.service'.format(path, service_name)

        if not client.file_exists(template_file):
            # Given template doesn't exist so we are probably using system init scripts
            return

        if target_name is not None:
            service_name = target_name

        params.update({'SERVICE_NAME': ExtensionsToolbox.remove_prefix(service_name, 'ovs-'),
                       'RUN_FILE_DIR': self._run_file_dir,
                       'STARTUP_DEPENDENCY': '' if startup_dependency is None else 'started {0}'.format(startup_dependency)})
        template_content = client.file_read(template_file)
        for key, value in params.iteritems():
            template_content = template_content.replace('<{0}>'.format(key), str(value))
        client.file_write('/etc/init/{0}.conf'.format(service_name), template_content)

        if delay_registration is False:
            self.register_service(service_metadata=params, node_name=self._system.get_my_machine_id(client))
        return params

    def get_service_status(self, name, client):
        """
        Retrieve the status of a service
        :param name: Name of the service to retrieve the status of
        :type name: str
        :param client: Client on which to retrieve the status
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The status of the service
        :rtype: str
        """
        try:
            name = self._get_name(name, client)
            output = client.run(['service', name, 'status'], allow_nonzero=True)
            # Special cases (especially old SysV ones)
            if 'rabbitmq' in name:
                status = re.search('\{pid,\d+?\}', output) is not None
                if status is True:
                    return 'active'
                return 'inactive'
            # Normal cases - or if the above code didn't yield an outcome
            if 'start/running' in output or 'is running' in output:
                return 'active'
            if 'stop' in output or 'not running' in output:
                return 'inactive'
            return output
        except CalledProcessError as ex:
            self._logger.exception('Get {0}.service status failed: {1}'.format(name, ex))
            raise Exception('Retrieving status for service "{0}" failed'.format(name))

    def start_service(self, name, client, timeout=5):
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
        if self.get_service_status(name, client) == 'active':
            return

        name = self._get_name(name, client)
        timeout = timeout if timeout > 0 else 5
        try:
            client.run(['service', name, 'start'])
            counter = 0
            while counter < timeout * 4:
                if self.get_service_status(name=name, client=client) == 'active':
                    return
                time.sleep(0.25)
                counter += 1
        except CalledProcessError as cpe:
            self._logger.exception('Start {0} failed, {1}'.format(name, cpe.output))
            raise
        raise RuntimeError('Did not manage to start service {0} on node with IP {1}'.format(name, client.ip))

    def stop_service(self, name, client, timeout=5):
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
        """
        if self.get_service_status(name, client) == 'inactive':
            return

        name = self._get_name(name, client)
        timeout = timeout if timeout > 0 else 5
        try:
            client.run(['service', name, 'stop'])
            counter = 0
            while counter < timeout * 4:
                if self.get_service_status(name=name, client=client) == 'inactive':
                    return
                time.sleep(0.25)
                counter += 1
        except CalledProcessError as cpe:
            self._logger.exception('Stop {0} failed, {1}'.format(name, cpe.output))
            raise
        raise RuntimeError('Did not manage to stop service {0} on node with IP {1}'.format(name, client.ip))

    def restart_service(self, name, client, timeout=5):
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
        """
        self.stop_service(name, client, timeout)
        self.start_service(name, client, timeout)

    def get_service_pid(self, name, client):
        """
        Retrieve the PID of a service
        :param name: Name of the service to retrieve the PID for
        :type name: str
        :param client: Client on which to retrieve the PID for the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The PID of the service or 0 if no PID found
        :rtype: int
        """
        name = self._get_name(name, client)
        if self.get_service_status(name, client) == 'active':
            output = client.run(['service', name, 'status'])
            if output:
                # Special cases (especially old SysV ones)
                if 'rabbitmq' in name:
                    match = re.search('\{pid,(?P<pid>\d+?)\}', output)
                else:
                    # Normal cases - or if the above code didn't yield an outcome
                    match = re.search('start/running, process (?P<pid>\d+)', output)
                if match is not None:
                    match_groups = match.groupdict()
                    if 'pid' in match_groups:
                        return match_groups['pid']
        return -1

    @classmethod
    def list_services(cls, client, add_status_info=False):
        """
        List all created services on a system
        :param client: Client on which to list all the services
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param add_status_info: Add status information of service in the output
        :type add_status_info: bool
        :return: List of all services which have been created at some point
        :rtype: generator
        """
        _ = add_status_info
        for filename in client.dir_list('/etc/init'):
            if filename.endswith('.conf'):
                yield filename.replace('.conf', '')

    @classmethod
    def monitor_services(cls):
        """
        Monitor the local OVS services
        :return: None
        :rtype: NoneType
        """
        try:
            previous_output = None
            while True:
                # Gather service states
                running_services = {}
                non_running_services = {}
                longest_service_name = 0
                for service_info in check_output('initctl list', shell=True).splitlines():
                    if not service_info.startswith('ovs-'):
                        continue
                    service_info = service_info.split(',')[0].strip()
                    service_name = service_info.split()[0].strip()
                    service_state = service_info.split()[1].strip()
                    if service_state == "start/running":
                        running_services[service_name] = service_state
                    else:
                        non_running_services[service_name] = service_state

                    if len(service_name) > longest_service_name:
                        longest_service_name = len(service_name)

                # Put service states in list
                output = ['OVS running processes',
                          '=====================\n']
                for service_name in sorted(running_services, key=lambda service: ExtensionsToolbox.advanced_sort(service, '_')):
                    output.append('{0} {1} {2}'.format(service_name, ' ' * (longest_service_name - len(service_name)), running_services[service_name]))

                output.extend(['\n\nOVS non-running processes',
                               '=========================\n'])
                for service_name in sorted(non_running_services, key=lambda service: ExtensionsToolbox.advanced_sort(service, '_')):
                    output.append('{0} {1} {2}'.format(service_name, ' ' * (longest_service_name - len(service_name)), non_running_services[service_name]))

                # Print service states (only if changes)
                if previous_output != output:
                    print '\x1b[2J\x1b[H'
                    for line in output:
                        print line
                    previous_output = list(output)
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def get_config_parser():
        # type: () -> ConfigParser
        """
        Retrieve the config parser for the implementation type
        :return: A config parser instance
        :rtype: ConfigParser
        """
        raise NotImplementedError('No config parser for upstart. Use the `extract_from_service_file` instead or implement the config parser.')

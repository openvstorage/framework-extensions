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
Systemd module
"""

import os
import time
from subprocess import CalledProcessError, check_output
from .base import ServiceAbstract
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ConfigParser import ConfigParser, NoOptionError, DEFAULTSECT


class Systemd(ServiceAbstract):
    """
    Contains all logic related to Systemd services
    """

    SERVICE_DIR = os.path.join(os.path.sep, 'lib', 'systemd', 'system')
    SERVICE_SUFFIX = '.service'
    SYSTEM_SERVICE_DIR = SERVICE_DIR

    @classmethod
    def list_services(cls, client):
        """
        List all created services on a system
        :param client: Client on which to list all the services
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: List of all services which have been created at some point
        :rtype: generator
        """
        for service_info in client.run(['systemctl', 'list-unit-files', '--type=service', '--no-legend', '--no-pager', '--full']).splitlines():
            yield '.'.join(service_info.split(' ')[0].split('.')[:-1])

    def add_service(self, name, client, params=None, target_name=None, startup_dependency=None, delay_registration=False):
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
        if params is None:
            params = {}

        service_name = self._get_name(name, client, self._config_template_dir.format('systemd'))
        template_file = '{0}/{1}.service'.format(self._config_template_dir.format('systemd'), service_name)

        if not client.file_exists(template_file):
            # Given template doesn't exist so we are probably using system init scripts
            return

        if target_name is not None:
            service_name = target_name

        params.update({'SERVICE_NAME': ExtensionsToolbox.remove_prefix(service_name, 'ovs-'),
                       'STARTUP_DEPENDENCY': '' if startup_dependency is None else '{0}.service'.format(startup_dependency)})
        template_content = client.file_read(template_file)
        for key, value in params.iteritems():
            template_content = template_content.replace('<{0}>'.format(key), str(value))
        service_path = self.get_service_file_path(service_name)
        client.file_write(service_path, template_content)

        try:
            client.run(['systemctl', 'daemon-reload'])
            client.run(['systemctl', 'enable', '{0}.service'.format(service_name)])
        except CalledProcessError as cpe:
            self._logger.exception('Add {0}.service failed, {1}'.format(service_name, cpe.output))
            raise Exception('Add {0}.service failed, {1}'.format(service_name, cpe.output))

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
        name = self._get_name(name, client)
        return client.run(['systemctl', 'is-active', name], allow_nonzero=True)

    def remove_service(self, name, client, delay_unregistration=False):
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
        try:
            client.run(['systemctl', 'disable', '{0}.service'.format(name)])
        except CalledProcessError:
            pass  # Service already disabled
        service_path = self.get_service_file_path(name)
        client.file_delete(service_path)
        client.run(['systemctl', 'daemon-reload'])

        if delay_unregistration is False:
            self.unregister_service(service_name=name, node_name=self._system.get_my_machine_id(client))

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

        try:
            # When service files have been adjusted, a reload is required for these changes to take effect
            client.run(['systemctl', 'daemon-reload'])
        except CalledProcessError:
            pass

        name = self._get_name(name, client)
        timeout = timeout if timeout > 0 else 5
        try:
            client.run(['systemctl', 'start', '{0}.service'.format(name)])
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
            client.run(['systemctl', 'stop', '{0}.service'.format(name)])
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
        try:
            # When service files have been adjusted, a reload is required for these changes to take effect
            client.run(['systemctl', 'daemon-reload'])
        except CalledProcessError:
            pass

        name = self._get_name(name, client)
        timeout = timeout if timeout > 0 else 5
        try:
            client.run(['systemctl', 'restart', '{0}.service'.format(name)])
            counter = 0
            while counter < timeout * 4:
                if self.get_service_status(name=name, client=client) == 'active':
                    return
                time.sleep(0.25)
                counter += 1
        except CalledProcessError as cpe:
            self._logger.exception('Restart {0} failed, {1}'.format(name, cpe.output))
            raise
        raise RuntimeError('Did not manage to restart service {0} on node with IP {1}'.format(name, client.ip))

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
        pid = 0
        name = self._get_name(name, client)
        if self.get_service_status(name, client) == 'active':
            output = client.run(['systemctl', 'show', name, '--property=MainPID']).split('=')
            if len(output) == 2:
                pid = output[1]
                if not pid.isdigit():
                    pid = 0
        return int(pid)

    def monitor_services(self):
        """
        Monitor the local services
        :return: None
        :rtype: NoneType
        """
        try:
            grep = ['egrep "{0}"'.format(prefix) for prefix in self._monitor_prefixes]
            previous_output = None
            while True:
                # Gather service states
                running_services = {}
                non_running_services = {}
                longest_service_name = 0
                for service_name in check_output('systemctl list-unit-files --full --type=service --no-legend --no-pager | {0} | tr -s " " | cut -d " " -f 1'.format(' | '.join(grep)), shell=True).splitlines():
                    try:
                        service_state = check_output('systemctl is-active {0}'.format(service_name), shell=True).strip()
                    except CalledProcessError as cpe:
                        service_state = cpe.output.strip()

                    service_name = service_name.replace('.service', '')
                    if service_state == 'active':
                        service_pid = check_output('systemctl show {0} --property=MainPID'.format(service_name), shell=True).strip().split('=')[1]
                        running_services[service_name] = (service_state, service_pid)
                    else:
                        non_running_services[service_name] = service_state

                    if len(service_name) > longest_service_name:
                        longest_service_name = len(service_name)

                # Put service states in list
                output = ['Running processes',
                          '=================\n']
                for service_name in sorted(running_services, key=lambda service: ExtensionsToolbox.advanced_sort(service, '_')):
                    output.append('{0} {1} {2}  {3}'.format(service_name, ' ' * (longest_service_name - len(service_name)), running_services[service_name][0], running_services[service_name][1]))

                output.extend(['\n\nNon-running processes',
                               '=====================\n'])
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
        raise SystemdUnitParser()


class SystemdUnitParser(ConfigParser):
    # @TODO handle multiple options!!!
    # Not an issue for Andes (no multiple options in config files)

    def __init__(self):
        """
        All option names are passed through the optionxform() method. Its default implementation converts option names to lower case.
        """
        ConfigParser.__init__(self)
        self.optionxform = lambda x: x  # Return the value passed

    def write(self, fp):
        """
        Write an Systemd .ini-alike format representation of the configuration state.
        """
        if self._defaults:
            fp.write("[{0}]\n".format(DEFAULTSECT))
            for key, value in self._defaults.iteritems():
                fp.write("{0}={1}\n".format(key, str(value).replace('\n', '\n\t')))
            fp.write("\n")
        for section in self._sections:
            fp.write("[{0}]\n".format(section))
            for key, value in self._sections[section].iteritems():
                if key == "__name__":
                    continue
                if value is not None or self._optcre == self.OPTCRE:
                    key = "{0}={1}".format(key, str(value).replace('\n', '\n\t'))
                fp.write("{0}\n".format(key))
            fp.write("\n")

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
Debian Package module
"""

import re
import collections
from distutils.version import LooseVersion
from subprocess import check_output, CalledProcessError
from ovs_extensions.log.logger import Logger


class DebianPackage(object):
    """
    Contains all logic related to Debian packages (used in e.g. Debian, Ubuntu)
    """
    _logger = Logger('extensions')
    APT_CONFIG_STRING = '-o Dir::Etc::sourcelist="sources.list.d/ovsaptrepo.list"'

    def __init__(self, packages, versions):
        self._packages = packages
        self._versions = versions

    @property
    def package_names(self):
        return self._packages['names']

    @staticmethod
    def get_release_name(client=None):
        """
        Get the release name based on the name of the repository
        :param client: Client on which to check the release name
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: Release name
        :rtype: str
        """
        command = "cat /etc/apt/sources.list.d/ovsaptrepo.list | grep openvstorage | cut -d ' ' -f 3"
        if client is None:
            output = check_output(command, shell=True).strip()
        else:
            output = client.run(command, allow_insecure=True).strip()
        return output.replace('-', ' ').title()

    def get_installed_versions(self, client=None, package_names=None):
        """
        Retrieve currently installed versions of the packages provided (or all if none provided)
        :param client: Client on which to check the installed versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: Name of the packages to check
        :type package_names: list
        :return: Package installed versions
        :rtype: dict
        """
        versions = collections.OrderedDict()
        if package_names is None:
            package_names = self._packages['names']
        for package_name in sorted(package_names):
            command = "dpkg -l {0} | awk '/^ii/ {{print $3}}'".format(package_name.replace(r"'", r"'\''"))  # ^ii means, line must start with ii, which means package is installed successfully
            if client is None:
                output = check_output(command, shell=True).strip()
            else:
                output = client.run(command, allow_insecure=True).strip()
            if output:
                versions[package_name] = LooseVersion(output)
        return versions

    @classmethod
    def get_candidate_versions(cls, client, package_names):
        """
        Retrieve the versions candidate for installation of the packages provided
        :param client: Root client on which to check the candidate versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: Name of the packages to check
        :type package_names: list
        :return: Package candidate versions
        :rtype: dict
        """
        cls.update(client=client)
        versions = collections.OrderedDict()
        for package_name in sorted(package_names):
            output = client.run(['apt-cache', 'policy', package_name, DebianPackage.APT_CONFIG_STRING]).strip()
            match = re.match(".*Installed: (?P<installed>\S+).*Candidate: (?P<candidate>\S+).*",
                             output, re.DOTALL)
            if match is not None:
                groups = match.groupdict()
                if groups['candidate'] == '(none)' and groups['installed'] == '(none)':
                    continue
                versions[package_name] = LooseVersion(groups['candidate']) if groups['candidate'] != '(none)' else ''
        return versions

    def get_binary_versions(self, client, package_names):
        """
        Retrieve the versions for the binaries related to the package_names
        :param client: Root client on which to retrieve the binary versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: Names of the packages
        :type package_names: list
        :return: Binary versions
        :rtype: dict
        """
        versions = collections.OrderedDict()
        for package_name in sorted(package_names):
            if package_name in ['alba', 'alba-ee']:
                versions[package_name] = LooseVersion(client.run(self._versions['alba'], allow_insecure=True))
            elif package_name == 'arakoon':
                versions[package_name] = LooseVersion(client.run(self._versions['arakoon'], allow_insecure=True))
            elif package_name in ['volumedriver-no-dedup-base', 'volumedriver-no-dedup-server',
                                  'volumedriver-ee-base', 'volumedriver-ee-server']:
                versions[package_name] = LooseVersion(client.run(self._versions['storagedriver'], allow_insecure=True))
            else:
                raise ValueError('Only the following packages in the OpenvStorage repository have a binary file: "{0}"'.format('", "'.join(self._packages['binaries'])))
        return versions

    def install(self, package_name, client):
        """
        Install the specified package
        :param package_name: Name of the package to install
        :type package_name: str
        :param client: Root client on which to execute the installation of the package
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        """
        if client.username != 'root':
            raise RuntimeError('Only the "root" user can install packages')

        command = "aptdcon --hide-terminal --allow-unauthenticated --install '{0}'".format(package_name.replace(r"'", r"'\''"))
        try:
            output = client.run('yes | {0}'.format(command), allow_insecure=True)
            if 'ERROR' in output:
                raise Exception('Installing package {0} failed. Command used: "{1}". Output returned: {2}'.format(package_name, command, output))
        except CalledProcessError as cpe:
            self._logger.warning('{0}: Install failed, trying to reconfigure the packages: {1}'.format(client.ip, cpe.output))
            client.run(['aptdcon', '--fix-install', '--hide-terminal', '--allow-unauthenticated'])
            self._logger.debug('{0}: Trying to install the package again'.format(client.ip))
            output = client.run('yes | {0}'.format(command), allow_insecure=True)
            if 'ERROR' in output:
                raise Exception('Installing package {0} failed. Command used: "{1}". Output returned: {2}'.format(package_name, command, output))

    @staticmethod
    def update(client):
        """
        Run the 'aptdcon --refresh' command on the specified node to update the package information
        :param client: Root client on which to update the package information
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        """
        if client.username != 'root':
            raise RuntimeError('Only the "root" user can update packages')
        try:
            client.run(['which', 'aptdcon'])
        except CalledProcessError:
            raise Exception('APT Daemon package is not correctly installed')
        try:
            client.run(['aptdcon', '--refresh', '--sources-file=ovsaptrepo.list'])
        except CalledProcessError as cpe:
            DebianPackage._logger.warning('{0}: Update package cache failed, trying to reconfigure the packages: {1}'.format(client.ip, cpe.output))
            client.run(['aptdcon', '--fix-install', '--hide-terminal', '--allow-unauthenticated'])
            DebianPackage._logger.debug('{0}: Trying to update the package cache again'.format(client.ip))
            client.run(['aptdcon', '--refresh', '--sources-file=ovsaptrepo.list'])

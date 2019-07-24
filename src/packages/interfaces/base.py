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
Debian Package module
"""

import collections
from distutils.version import LooseVersion
from abc import ABCMeta, abstractmethod
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.log.logger import Logger

# noinspection PyUnreachableCode
if False:
    from typing import List, Dict


class PackageManagerBase(object):
    """
    Contains all logic related to Debian packages (used in e.g. Debian, Ubuntu)
    """
    __metaclass__ = ABCMeta

    _logger = Logger('extensions')

    def __init__(self, package_info):
        self.package_info = package_info

    @staticmethod
    def get_release_name(client=None):
        # type: (SSHClient) -> str
        """
        Get the release name based on the name of the repository
        :param client: Client on which to check the release name
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: Release name
        :rtype: str
        """
        raise NotImplementedError()

    @abstractmethod
    def get_installed_versions(self, client=None, package_names=None):
        # type: (SSHClient, List[str]) -> Dict[str, str]
        """
        Retrieve currently installed versions of the packages provided (or all if none provided)
        :param client: Client on which to check the installed versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: Name of the packages to check
        :type package_names: list
        :return: Package installed versions
        :rtype: dict
        """

    @classmethod
    def get_candidate_versions(cls, client, package_names):
        # type: (SSHClient, List[str]) -> Dict[str, str]
        """
        Retrieve the versions candidate for installation of the packages provided
        :param client: Root client on which to check the candidate versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: Name of the packages to check
        :type package_names: list
        :return: Package candidate versions
        :rtype: dict
        """
        raise NotImplementedError()

    def get_binary_versions(self, client, package_names=None):
        """
        Retrieve the versions for the binaries related to the package_names
        :param client: Root client on which to retrieve the binary versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_names: Names of the packages
        :type package_names: list
        :return: Binary versions
        :rtype: dict
        """
        if package_names is None:
            package_names = set()
            for names in self.package_info['binaries'].itervalues():
                package_names = package_names.union(names)

        versions = collections.OrderedDict()
        version_commands = self.package_info['version_commands']
        for package_name in sorted(package_names):
            if package_name not in version_commands:
                raise ValueError('Only the following packages in the OpenvStorage repository have a binary file: "{0}"'.format('", "'.join(sorted(version_commands.keys()))))
            versions[package_name] = LooseVersion(client.run(version_commands[package_name], allow_insecure=True))
        return versions

    @abstractmethod
    def install(self, package_name, client):
        # type: (str, SSHClient) -> None
        """
        Install the specified package
        :param package_name: Name of the package to install
        :type package_name: str
        :param client: Root client on which to execute the installation of the package
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        """

    @staticmethod
    def update(client):
        # type: (SSHClient) -> None
        """
        Update the package information
        :param client: Root client on which to update the package information
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        """
        raise NotImplementedError()

    @staticmethod
    def validate_client(client, msg='Only the "root" user can manage packages'):
        # type: (SSHClient, str) -> None
        """
        Validate if the client can manage packages
        :return: None
        :raises RuntimeError if the client cannot manage any packages
        """
        if client.username != 'root':
            raise RuntimeError(msg)

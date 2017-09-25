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
Centos OS module
"""

from subprocess import CalledProcessError, check_output


class Centos(object):
    """
    Contains all logic related to Centos specific
    """
    def __init__(self, configuration, system):
        """
        Constructor
        """
        self._configuration = configuration
        self._system = system

    def get_path(self, binary_name):
        """
        Retrieve the absolute path for binary
        :param binary_name: Binary to get path for
        :return: Path
        """
        machine_id = self._system.get_my_machine_id()
        config_location = '/ovs/framework/hosts/{0}/paths|{1}'.format(machine_id, binary_name)
        if not self._configuration.exists(config_location):
            try:
                path = check_output("which '{0}'".format(binary_name.replace(r"'", r"'\''")), shell=True).strip()
                self._configuration.set(config_location, path)
            except CalledProcessError:
                return None
        else:
            path = self._configuration.get(config_location)
        return path

    @staticmethod
    def get_fstab_entry(device, mp, filesystem='ext4'):
        """
        Retrieve fstab entry for mountpoint
        :param device: Device in fstab
        :param mp: Mountpoint
        :param filesystem: Filesystem of entry
        :return: Fstab entry
        """
        return '{0}    {1}         {2}    defaults,nofail,noatime,discard    0    2'.format(device, mp, filesystem)

    @staticmethod
    def get_ip_addresses(client, remove_local_host_ips=True):
        """
        Retrieve the currently configured IP addresses on the SSHClient provided
        :param client: The SSHClient to retrieve the IP addresses for
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param remove_local_host_ips: Remove the local host IPs, eg: 127.0.0.1
        :type remove_local_host_ips: bool
        :return: A list of IP addresses available on the SSHClient
        :rtype: list
        """
        raise NotImplementedError()

    @staticmethod
    def get_ssh_service_name():
        """
        Retrieve SSH service name
        :return: SSH service name
        """
        return 'sshd'

    @staticmethod
    def get_openstack_web_service_name():
        """
        Retrieve openstack webservice name
        :return: Openstack webservice name
        """
        return 'httpd'

    @staticmethod
    def get_openstack_cinder_service_name():
        """
        Retrieve openstack cinder service name
        :return: Openstack cinder service name
        """
        return 'openstack-cinder-volume'

    @staticmethod
    def get_openstack_services():
        """
        Retrieve openstack services
        :return: Openstack services
        """
        return ['openstack-nova-compute', 'openstack-cinder-volume', 'openstack-cinder-api']

    @staticmethod
    def get_openstack_users():
        """
        Retrieve openstack users
        :return: Openstack users
        """
        return ['qemu', 'cinder', 'nova']

    @staticmethod
    def get_openstack_package_base_path():
        """
        Retrieve openstack package base path
        :return: Openstack package base path
        """
        return '/usr/lib/python2.7/site-packages'

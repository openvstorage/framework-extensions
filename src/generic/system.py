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
Generic system module, executing statements on local node
"""

import os
import re
from subprocess import check_output
from ovs_extensions.constants import is_unittest_mode
from ovs_extensions.generic.sshclient import SSHClient


class System(object):
    """
    Generic helper class
    """
    def __init__(self):
        """
        Dummy init method
        """
        raise RuntimeError('System is a static class')

    @classmethod
    def get_my_machine_id(cls, client=None):
        """
        Returns unique machine id, generated at install time.
        :param client: Remote client on which to retrieve the machine ID
        :type client: SSHClient
        :return: Machine ID
        :rtype: str
        """
        cmd = ['cat', '/etc/openvstorage_id']
        if client is None:
            out = check_output(cmd)
        else:
            out = client.run(cmd)
        return out.strip()

    @classmethod
    def generate_id(cls, product='openvstorage', client=None):
        """
        Generates a certain ID for a given product. This ID will be saved under /etc/PRODUCT_id
        :param product: Product to generate the ID for
        :type product: str
        :param client: Client to use for ID generation
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The generated ID
        :rtype: str
        """
        id_file_path = os.path.join('/etc', '{0}_id'.format(product))
        new_id = check_output('openssl rand -base64 64 | tr -dc A-Z-a-z-0-9 | head -c 16', shell=True)
        if client is None:
            if os.path.exists(id_file_path):
                raise RuntimeError('An ID has already been generated for product {0}'.format(product))
            with open(id_file_path, 'w') as id_file:
                id_file.write(new_id)
        else:
            if client.file_exists(id_file_path):
                raise RuntimeError('An ID has already been generated for product {0}'.format(product))
            client.file_write(id_file_path, new_id)
        return new_id

    @classmethod
    def update_hosts_file(cls, ip_hostname_map, client):
        """
        Update/add entry for hostname ip in /etc/hosts
        :param ip_hostname_map: Mapping between IPs and their host names
        :type ip_hostname_map: dict

        :param client: Remote client on which to update the hosts file
        :type client: SSHClient

        :return: None
        """
        for ip, hostnames in ip_hostname_map.iteritems():
            if re.match(r'^localhost$|^127(?:\.[0-9]{1,3}){3}$|^::1$', ip):
                # Never update loop-back addresses
                continue

            contents = client.file_read('/etc/hosts').strip() + '\n'

            if isinstance(hostnames, list):
                hostnames = ' '.join(hostnames)

            result = re.search('^{0}\s.*\n'.format(ip), contents, re.MULTILINE)
            if result:
                if hostnames not in result.groups(0):
                    contents = contents.replace(result.group(0), '{0} {1}\n'.format(ip, hostnames))
            else:
                contents += '{0} {1}\n'.format(ip, hostnames)

            client.file_write('/etc/hosts', contents)

    @classmethod
    def ports_in_use(cls, client=None):
        """
        Returns the ports in use
        :param client: Remote client on which to retrieve the ports in use
        :type client: SSHClient

        :return: Ports in use
        :rtype: list
        """
        cmd = "netstat -ln | sed 1,2d | sed 's/\s\s*/ /g' | cut -d ' ' -f 4 | cut -d ':' -f 2"
        if client is None:
            output = check_output(cmd, shell=True)
        else:
            output = client.run(cmd, allow_insecure=True)
        for found_port in output.splitlines():
            if found_port.isdigit():
                yield int(found_port.strip())

    @classmethod
    def get_free_ports(cls, selected_range, exclude=None, amount=1, client=None):
        """
        Return requested amount of free ports not currently in use and not within excluded range
        :param selected_range: The range in which the amount of free ports need to be fetched
                               e.g. '2000-2010' or '5000-6000, 8000-8999' ; note single port extends to [port -> 65535]
        :type selected_range: list
        :param exclude: List of port numbers which should be excluded from the calculation
        :type exclude: list
        :param amount: Amount of free ports requested
                       if amount == 0: return all the available ports within the requested range
        :type amount: int
        :param client: SSHClient to node
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :raises ValueError: If requested amount of free ports could not be found
        :return: Sorted incrementing list of the requested amount of free ports
        :rtype: list
        """
        unittest_mode = is_unittest_mode()
        requested_range = []
        for port_range in selected_range:
            if isinstance(port_range, list):
                current_range = [port_range[0], port_range[1]]
            else:
                current_range = [port_range, 65535]
            if 0 <= current_range[0] <= 1024:
                current_range = [1025, current_range[1]]
            requested_range += range(current_range[0], current_range[1] + 1)

        free_ports = []
        if exclude is None:
            exclude = []
        exclude_list = list(exclude)

        if unittest_mode:
            ports_in_use = []
        else:
            ports_in_use = cls.ports_in_use(client)
        exclude_list += ports_in_use

        if unittest_mode:
            start_end = [0, 0]
        else:
            cmd = 'cat /proc/sys/net/ipv4/ip_local_port_range'
            if client is None:
                output = check_output(cmd, shell=True)
            else:
                output = client.run(cmd.split())
            start_end = map(int, output.split())
        ephemeral_port_range = xrange(min(start_end), max(start_end))
        for possible_free_port in requested_range:
            if possible_free_port not in ephemeral_port_range and possible_free_port not in exclude_list:
                free_ports.append(possible_free_port)
                if len(free_ports) == amount:
                    return free_ports
        if amount == 0:
            return free_ports
        raise ValueError('Unable to find the requested amount of free ports')

    @staticmethod
    def get_component_identifier():
        # type: () -> str
        """
        Retrieve the identifier of the component
        :return: The ID of the component
        :rtype: str
        """
        raise NotImplementedError('The generic implmentation has no record of which component it is used in')

# Copyright (C) 2018 iNuron NV
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
IPMI calls
"""
import time
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System

IPMI_INFO_LOCATION = 'ovs/alba/asdnodes/{0}/config/ipmi'
SLEEPTIME = 3
IPMI_POWER_ON = 'on'
IPMI_POWER_OFF = 'off'


class IPMIController():
    """
    Controller class that can execute ipmi calls
    """

    def __init__(self, node_id):
        ipmi_info = dict(Configuration.get(IPMI_INFO_LOCATION.format(node_id)))
        self.ip = ipmi_info.get('ip')
        self.username = ipmi_info.get('username')
        self.pwd = ipmi_info.get('password')

        self.basic_command = ['ipmi-power', '-h', self.ip, '-u', self.username, '-p', self.pwd]

        self._local_client = SSHClient(System.get_my_storagerouter())

    @staticmethod
    def _parse_status(status_string):
        status_dict = {}
        for line in status_string.split('\n'):
            key, value = line.split(':')
            key = key.strip()
            value = value.strip()
            status_dict[key] = value
        return status_dict

    def power_off_node(self, retries=10):
        """
        This function will shut down the provided node using the package freeipmi
        :return: <node_ip>: ok
        :raises: upon timeout
        :raises: if something went wrong calling the command
        """
        command = self.basic_command + ['-f']
        try:
            out, err = self._local_client.run(command)
            return err
        except ValueError:
            out = self._local_client.run(command)
            out = IPMIController._parse_status(status_string=out)
            for i in xrange(retries):
                if self.status_node().get(self.ip) == IPMI_POWER_OFF:
                    return out
                time.sleep(SLEEPTIME)
            raise RuntimeError('Shutting down node {0} failed after {1} seconds'.format(self.ip, SLEEPTIME * retries))

    def power_on_node(self, retries=10):
        """
        This function will power on the provided node using the package freeipmi
        :return: <node_ip>: ok
        :raises: upon timeout
        :raises: if something went wrong calling the command
        """
        command = self.basic_command + ['-n']
        try:
            out, err = self._local_client.run(command)
            return err
        except ValueError:
            out = self._local_client.run(command)
            out = IPMIController._parse_status(status_string=out)
            for i in xrange(retries):
                if self.status_node().get(self.ip) == IPMI_POWER_ON:
                    return out
                time.sleep(SLEEPTIME)
            raise RuntimeError('Shutting down node {0} failed after {1} seconds'.format(self.ip, SLEEPTIME * retries))

    def status_node(self):
        """
        This function will call the power status of the provided node using the package "freeipmi"
        :return: <node_ip>: on/off
        """
        command = self.basic_command + ['-s']
        try:
            out, err = self._local_client.run(command)
        except ValueError:
            out = self._local_client.run(command)
            out = IPMIController._parse_status(status_string=out)
        return out

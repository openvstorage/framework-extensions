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
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.generic.configuration import Configuration

IPMI_INFO_LOCATION = 'ovs/alba/asdnodes/{0}/config/ipmi'


class IPMIController():
    """
    Controller class that can execute ipmi calls
    """

    def __init__(self, node_id):
        ipmi_info = dict(Configuration.get(IPMI_INFO_LOCATION.format(node_id)))
        self.ip = ipmi_info.get('ip')
        self.username = ipmi_info.get('username')
        self.pwd = ipmi_info.get('password')

        self._local_client = SSHClient(System.get_my_storagerouter())

    @staticmethod
    def _parse_status(status_string):
        status_dict = {}
        print status_string.split('\n')
        for line in status_string.split('\n'):
            key, value = line.split(':')
            key = key.strip()
            status_dict[key] = value
        return status_dict

    def power_off_node(self):
        """
        This function will shut down the provided node using the package freeipmi
        :return:
        """
        command = ['ipmi-power', '-h', self.ip , '-u', self.username, '-p', self.pwd, '-f']
        try:
            out, err = self._local_client.run(command)
        except ValueError:
            out = self._local_client.run(command)
            out = IPMIController._parse_status(status_string=out)
        return out

    def status_node_freeipmi(self):
        """
        This function will call the power status of the provided node using the package "freeipmi"
        :return: <node_id>: on/off
        """
        command = ['ipmi-power', '-h', self.ip , '-u', self.username, '-p', self.pwd, '-s']
        try:
            out, err = self._local_client.run(command)
        except ValueError:
            out = self._local_client.run(command)
            out = IPMIController._parse_status(status_string=out)
        return out

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
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from subprocess import CalledProcessError


class IPMICallException(Exception):
    """
    Custom Exception to informat that an IPMI call failed
    """
    pass


class IPMITimeOutException(Exception):
    """
    Custom exception to inform that an IPMI call timed out
    """
    pass


class IPMIController(object):
    """
    Controller class that can execute ipmi calls
    """

    IPMI_INFO_LOCATION = 'ovs/alba/asdnodes/{0}/config/ipmi'
    SLEEPTIME = 3
    IPMI_POWER_ON = 'on'
    IPMI_POWER_OFF = 'off'

    def __init__(self, ip, username, password, client):
        actual_params = {'ip': ip,
                         'username': username,
                         'password': password,
                         'client': client}
        required_params = {'ip': (str, ExtensionsToolbox.regex_ip, True),
                           'username': (str, None, True),
                           'password': (str, None, True),
                           'client': (SSHClient, None, True)}
        ExtensionsToolbox.verify_required_params(actual_params=actual_params,
                                                 required_params=required_params)
        for key, value in actual_params.iteritems():
            if value is 'null':
                raise ValueError("Argument '{0}' cannot be 'null'".format(key))

        self.ip = ip
        self.username = username
        self.pwd = password
        self.basic_command = ['ipmi-power', '-h', self.ip, '-u', self.username, '-p', self.pwd]

        self._client = client

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
        Will shut down the provided node using the package freeipmi
        :return: {<node_ip>: ok}
        :raises: IPMITimeOutException upon timeout
        :raises: IPMIException if something went wrong calling the command
        """
        command = self.basic_command + ['-f']
        try:
            out = self._client.run(command)
            out = IPMIController._parse_status(status_string=out)
            for i in xrange(retries):
                if self.status_node().get(self.ip) == self.IPMI_POWER_OFF:
                    return out
                time.sleep(self.SLEEPTIME)
            raise IPMITimeOutException('Shutting down node {0} failed after {1} seconds'.format(self.ip, self.SLEEPTIME * retries))
        except CalledProcessError as ex:
            raise IPMICallException("Error '{0}' occurred using IPMI command '{1}'".format(ex.returncode, command))

    def power_on_node(self, retries=10):
        """
        This function will power on the provided node using the package freeipmi
        :return: {<node_ip>: ok}
        :raises: IPMITimeOutException upon timeout
        :raises: IPMIException if something went wrong calling the command
        """
        command = self.basic_command + ['-n']
        try:
            out = self._client.run(command)
            out = IPMIController._parse_status(status_string=out)
            for i in xrange(retries):
                if self.status_node().get(self.ip) == self.IPMI_POWER_ON:
                    return out
                time.sleep(self.SLEEPTIME)
            raise IPMITimeOutException('Shutting down node {0} failed after {1} seconds'.format(self.ip, self.SLEEPTIME * retries))
        except CalledProcessError as ex:
            raise IPMICallException("Error '{0}' occurred using command '{}'".format(ex.returncode, command))

    def status_node(self):
        """
        This function will call the power status of the provided node using the package "freeipmi"
        :return: <node_ip>: on/off
        :raises IPMICallException
        """
        command = self.basic_command + ['-s']
        try:
            out = self._client.run(command)
            out = IPMIController._parse_status(status_string=out)
            return out
        except CalledProcessError as ex:
            raise IPMICallException("Error '{0}' occurred using command '{1}'".format(ex.returncode, command))

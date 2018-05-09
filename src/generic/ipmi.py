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
from subprocess import CalledProcessError
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class IPMICallException(Exception):
    """
    Custom Exception to inform that an IPMI call failed
    """
    pass


class IPMITimeOutException(Exception):
    """
    Custom exception to inform that an IPMI call timed out
    """
    pass


class IPMIController(object):
    """
    Controller class that can execute IPMI calls
    Depends on 'freeipmi'
    """

    SLEEP_TIME = 3
    IPMI_POWER_ON = 'on'
    IPMI_POWER_OFF = 'off'

    def __init__(self, ip, username, password, client):
        # type: (str, str, str, SSHClient) -> None
        """
        Intialize an IPMIController
        :param ip: IP of the host to control through IPMI
        :type ip: str
        :param username: IPMI username of the host to control through IPMI
        :type username: str
        :param password: IPMI password of the host to control through IPMI
        :type password: str
        :param client: SSHClient to perform all IPMI commands on
        :type client: SSHClient
        """
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
        self.ip = ip
        self.username = username
        self._basic_command = ['ipmi-power', '-h', self.ip, '-u', self.username, '-p', self._pwd]
        self._client = client
        self._pwd = password

    @staticmethod
    def _parse_status(status_string):
        # type: (str) -> Dict[str, str]
        """
        Parses the string returned from IPMI commands
        :param status_string: String to parse eg x.x.x.x:off\n
        :return: A dict reflecting the string but in a usable manner
        :rtype: dict
        """
        status_dict = {}
        for line in status_string.split('\n'):
            key, value = line.split(':')
            key = key.strip()
            value = value.strip()
            status_dict[key] = value
        return status_dict

    def power_off_node(self, retries=10, retry_interval=SLEEP_TIME):
        # type: (int, float) -> Dict[str, str]
        """
        Power off the host
        :param retries: Number of tries to assert that the power state is off
        Total wait time could be <retries> * <retry_interval> in the worst case
        :type retries: int
        :param retry_interval: Wait time after a try (in seconds)
        :type retry_interval: float
        :return: {<node_ip>: <node_status>}
        :rtype: dict
        :raises: IPMITimeOutException upon timeout
        :raises: IPMIException if something went wrong calling the command
        """
        command = self._basic_command + ['-f']
        try:
            out = self._client.run(command)
            out = IPMIController._parse_status(status_string=out)
            for i in xrange(retries):
                if self.status_node().get(self.ip) == self.IPMI_POWER_OFF:
                    return out
                time.sleep(retry_interval)
            raise IPMITimeOutException('Shutting down node {0} failed after {1} seconds'.format(self.ip, retry_interval * retries))
        except CalledProcessError as ex:
            raise IPMICallException("Error '{0}' occurred using IPMI command '{1}'".format(ex.returncode, command))

    def power_on_node(self, retries=10, retry_interval=SLEEP_TIME):
        # type: (int, float) -> Dict[str, str]
        """
        Power on the host
        :param retries: Number of tries to assert that the power state is off
        Total wait time could be <retries> * <retry_interval> in the worst case
        :type retries: int
        :param retry_interval: Wait time after a try (in seconds)
        :type retry_interval: float
        :return: {<node_ip>: ok}
        :rtype: dict
        :raises: IPMITimeOutException upon timeout
        :raises: IPMIException if something went wrong calling the command
        """
        command = self._basic_command + ['-n']
        try:
            out = self._client.run(command)
            out = IPMIController._parse_status(status_string=out)
            for i in xrange(retries):
                if self.status_node().get(self.ip) == self.IPMI_POWER_ON:
                    return out
                time.sleep(retry_interval)
            raise IPMITimeOutException('Shutting down node {0} failed after {1} seconds'.format(self.ip, retry_interval * retries))
        except CalledProcessError as ex:
            raise IPMICallException("Error '{0}' occurred using command '{}'".format(ex.returncode, command))

    def status_node(self):
        # type: () -> Dict[str, str]
        """
        Retrieve power status of the host
        :return: {<node_ip>: on/off}
        :rtype: dict
        :rtype: dict(str, str)
        :raises IPMICallException
        """
        command = self._basic_command + ['-s']
        try:
            out = self._client.run(command)
            out = IPMIController._parse_status(status_string=out)
            return out
        except CalledProcessError as ex:
            raise IPMICallException("Error '{0}' occurred using command '{1}'".format(ex.returncode, command))

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

    @staticmethod
    def power_off_node(node_id):
        ip, username, pwd = Configuration.get(IPMI_INFO_LOCATION.format(node_id))
        _local_client = SSHClient(endpoint=System.get_my_storagerouter())
        out, err = _local_client.run(['ipmitool', '-I','lanplus', '-H',ip, '-U',username, '-P', pwd, 'chassis', 'power', 'off'])
        return out


    @staticmethod
    def status_node(node_id):
        ip, username, pwd = Configuration.get(IPMI_INFO_LOCATION.format(node_id))
        _local_client = SSHClient(System.get_my_storagerouter())
        out, err = _local_client.run(['ipmitool', '-I','lanplus', '-H',ip, '-U',username, '-P', pwd, 'chassis', 'status'])
        return out

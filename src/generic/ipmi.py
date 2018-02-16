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

from ovs_extensions.generic.sshclient import SSHClient
from source.tools.configuration import Configuration

IPMI_INFO_LOCATION = 'ovs/alba/asdnodes/{0}/config/ipmi'


class IPMIController():
    """
    Controller class that can execute ipmi calls
    """

    @staticmethod
    def power_off(node_id):
        ip, username, pwd = Configuration.get(IPMI_INFO_LOCATION.format(node_id))
        _local_client = SSHClient(endpoint=ip, username=username, password=pwd)
        out, err = _local_client.run(['ipmitool', '-I', '-H', '-U', '-P', 'chassis', 'power', 'off'])


    @staticmethod
    def status(node_id):
        ip, username, pwd = Configuration.get(IPMI_INFO_LOCATION.format(node_id))
        _local_client = SSHClient(endpoint=ip, username=username, password=pwd)
        out, err = _local_client.run(['ipmitool', '-I', '-H', '-U', '-P', 'chassis', 'status'])


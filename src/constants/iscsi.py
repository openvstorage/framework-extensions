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
Constants involved in config management
"""

import os

CONFIG_ISCSI = '/ovs/iscsi'

CONFIG_LOGGING = os.path.join(CONFIG_ISCSI, 'logging')                          #/ovs/iscsi/logging

CONFIG_ISCSI_NODE = os.path.join(CONFIG_ISCSI, 'iscsinodes')                    #/ovs/iscsi/iscsinodes
CONFIG_ISCSI_NODE_ID = os.path.join(CONFIG_ISCSI_NODE, '{0}')                   #/ovs/iscsi/iscsinodes/<node_id>
CONFIG_ISCSI_NODE_CONFIG = os.path.join(CONFIG_ISCSI_NODE_ID, 'config/main')    #/ovs/iscsi/iscsinodes/<node_id>/config/main

CONFIG_ISCSI_SERVICE_KEY = os.path.join(CONFIG_ISCSI_NODE_ID, '{1}')            #/ovs/iscsi/iscsinodes/<node_id>/services/<key>

CONFIG_TARGET = os.path.join(CONFIG_ISCSI_NODE_ID, 'target')                    #/ovs/iscsi/iscsinodes/<node_id>/target
CONFIG_TARGET_ID = os.path.join(CONFIG_TARGET, '{1}')                           #/ovs/iscsi/iscsinodes/<node_id>/target/<target_id>

CONFIG_TARGET_VDISK = os.path.join(CONFIG_TARGET_ID, 'vdisk')                   #/ovs/iscsi/iscsinodes/<node_id>/target/<target_id>/vdisk
CONFIG_TARGET_VDISK_ID = os.path.join(CONFIG_TARGET_VDISK, '{2}')               #/ovs/iscsi/iscsinodes/<node_id>/target/<target_id>/vdisk/<vd_guid>

CONFIG_TARGET_MAIN = os.path.join(CONFIG_ISCSI_NODE_ID, 'target/config/main')   #/ovs/iscsi/iscsinodes/<node_id>/target/config/main

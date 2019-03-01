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
OVS pathing constants
"""


BASE_OVS = 'ovs'
BASE_API = 'api'

OVS_DAL = '.'.join([BASE_OVS, 'dal'])
OVS_DAL_HYBRIDS = '.'.join([OVS_DAL, 'hybrids'])                                # ovs.dal.hybrids
OVS_DAL_HYBRIDS_FILE = '.'.join([OVS_DAL_HYBRIDS, '{0}'])                       # ovs.dal.hybrids.{0}

OVS_EXTENSIONS = '.'.join([BASE_OVS, 'extensions'])
RABBIT_MQ_MAPPINGS = '.'.join([OVS_EXTENSIONS, 'rabbitmq', 'mappings'])         # ovs.extensions.rabbitmq.mappings

OVS_LIB = '.'.join([BASE_OVS, 'lib'])                                           # ovs.lib
OVS_LIB_HELPERS = '.'.join([OVS_LIB, 'helpers'])                                # ovs.lib.helpers

API_VIEWS = '.'.join([BASE_API, 'backend', 'views'])                            # api.backends.views

SOURCE_DAL_OBJECTS = '.'.join(['source', 'dal', 'objects'])                     # source.dal.objects

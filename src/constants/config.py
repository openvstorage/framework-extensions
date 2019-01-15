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
Shared strings
"""
import os


ARAKOON_NAME = 'cacc'
ARAKOON_NAME_UNITTEST = 'unittest-cacc'

OVS_CONFIG = os.path.join('/opt', 'OpenvStorage', 'config')                                 # /opt/OpenvStorage/config
CACC_LOCATION = os.path.join(OVS_CONFIG, 'arakoon_cacc.ini')                                # /opt/OpenvStorage/config/arakoon_cacc.ini
CONFIG_STORE_LOCATION = os.path.join(OVS_CONFIG, 'framework.json')                          # /opt/OpenvStorage/config/framework.json
CONFIG_ARAKOON_LOCATION = os.path.join(OVS_CONFIG, 'arakoon_{0}.ini')                       # /opt/OpenvStorage/config/arakoon_{0}.ini

COMPONENTS_KEY = os.path.join('/ovs', 'machines', '{0}', 'components')                      # /ovs/machines/{0}/components

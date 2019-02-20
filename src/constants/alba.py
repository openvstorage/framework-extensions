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
Shared alba constants module
"""
import os

MAINTENANCE_PREFIX = 'alba-maintenance'

ALBA_BASE = os.path.join(os.path.sep, 'ovs', 'alba')                                                # /ovs/alba

BACKENDS_BASE = os.path.join(os.path.sep, ALBA_BASE, 'backends')                                    # /ovs/alba/backends
BACKEND_MAINTENANCE = os.path.join(os.path.sep, BACKENDS_BASE, '{0}', 'maintenance')                # /ovs/alba/backends/{0}/maintenance
BACKEND_MAINTENANCE_SERVICE = os.path.join(os.path.sep, BACKEND_MAINTENANCE, '{1}')                 # /ovs/alba/backends/{0}/maintenance/{1}
BACKEND_MAINTENANCE_CONFIG = os.path.join(os.path.sep, BACKEND_MAINTENANCE_SERVICE, 'config')       # /ovs/alba/backends/{0}/maintenance/{1}/config

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

FRAMEWORK_BASE = '/ovs/framework/'

### Remote config
REMOTE_CONFIG_BACKEND_BASE = 'ovs/framework/used_configs/alba_backends/'
REMOTE_CONFIG_BACKEND_CONFIG = os.path.join(REMOTE_CONFIG_BACKEND_BASE, '{0}/abm_config')
REMOTE_CONFIG_BACKEND_INI = '{0}.ini'.format(REMOTE_CONFIG_BACKEND_CONFIG)  # ovs/framework/remote_configs/alba_backends/{abe_guid}/abm_config.ini

### NBD related config paths
NBD = os.path.join(FRAMEWORK_BASE, 'nbdnodes')
NBD_ID = os.path.join(NBD, '{0}')


### SCRUBBER related config paths

SCRUB_KEY = '/ovs/framework/jobs/scrub'  # Parent key for all scrub related jobs
SCRUB_JOB = '{0}/{{0}}/job_info'.format(SCRUB_KEY)              # '/ovs/framework/jobs/scrub/{0}/job_info
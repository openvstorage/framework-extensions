# Copyright (C) 2017 iNuron NV
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
Manager module
"""

import logging

logger = logging.getLogger(__name__)


class Manager(object):
    """
    Contains all base logic related to packages
    """

    OVS_PACKAGE_NAMES = ['alba', 'alba-ee', 'arakoon',
                         'openvstorage', 'openvstorage-backend', 'openvstorage-sdm',
                         'volumedriver-no-dedup-base', 'volumedriver-no-dedup-server',
                         'volumedriver-ee-base', 'volumedriver-ee-server']
    OVS_PACKAGES_WITH_BINARIES = ['alba', 'alba-ee', 'arakoon', 'volumedriver-no-dedup-server', 'volumedriver-ee-server']

    GET_VERSION_ALBA = 'alba version --terse'
    GET_VERSION_ARAKOON = "arakoon --version | grep version: | awk '{print $2}'"
    GET_VERSION_STORAGEDRIVER = "volumedriver_fs --version | grep version: | awk '{print $2}'"

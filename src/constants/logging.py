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

# Logger names
CORE_LOGGER_NAME = 'ovs'
EXTENSIONS_LOGGER_NAME = 'ovs_extensions'

# Formatting
# Format is the name of the logger
LOG_FORMAT = '%(asctime)s - %(hostname)s - %(process)s/%(thread)d - {0}/%(filename)s - %(funcName)s - %(sequence)s - %(levelname)s - %(message)s'
# Testing purposes
LOG_FORMAT_NO_NAME = '%(asctime)s - %(hostname)s - %(process)s/%(thread)d - %(filename)s - %(funcName)s - %(sequence)s - %(levelname)s - %(message)s'

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
import os

# Logger names
CORE_LOGGER_NAME = 'ovs'
EXTENSIONS_LOGGER_NAME = 'ovs_extensions'

# Formatting
# Format is the name of the logger - still used for backwards compatibility (things importing the logger
LOG_FORMAT_OLD = '%(asctime)s - %(hostname)s - %(process)s/%(thread)d - {0}/%(filename)s - %(funcName)s - %(sequence)s - %(levelname)s - %(message)s'

LOG_FORMAT = '%(asctime)s - %(hostname)s - %(process)s/%(thread)d - %(name)s - %(funcName)s - %(sequence)s - %(levelname)s - %(message)s'

LOG_FORMAT_UNITTEST = '%(levelname)s - %(message)s'

# Target types
TARGET_TYPE_FILE = 'file'
TARGET_TYPE_REDIS = 'redis'
TARGET_TYPE_CONSOLE = 'console'
TARGET_TYPES = [TARGET_TYPE_FILE, TARGET_TYPE_REDIS, TARGET_TYPE_CONSOLE]

LOG_PATH = os.path.join(os.path.sep, 'var', 'log', 'ovs')
LOG_LEVELS = {0: 'NOTSET',
              10: 'DEBUG',
              20: 'INFO',
              30: 'WARNING',
              40: 'ERROR',
              50: 'CRITICAL'}

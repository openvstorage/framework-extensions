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
Exceptions module for Configuration
"""


class ConfigurationNoLockAvailableException(Exception):
    """
    Custom exception thrown when lock could not be acquired in time
    """
    pass


class ConfigurationNotFoundException(Exception):
    """
    Not found exception.
    Throw when a key could not be found
    """
    pass


class ConfigurationConnectionException(Exception):
    """
    Connection exception.
    Throw when no connection could be made
    """
    pass
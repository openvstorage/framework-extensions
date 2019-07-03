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

# Expose the Pyrakoon exceptions here too as the client uses them internally.
# noinspection PyUnresolvedReferences
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonAssertionFailed, ArakoonGoingDown, ArakoonNotFound,\
    ArakoonNodeNotMaster, ArakoonNoMaster, ArakoonNotConnected, ArakoonSocketException, ArakoonSockNotReadable,\
    ArakoonSockReadNoBytes, ArakoonSockSendError


class NoLockAvailableException(Exception):
    """
    Raised when the lock could not be acquired
    """
    pass

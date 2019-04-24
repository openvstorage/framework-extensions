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

from .exceptions import NoLockAvailableException
from .client import locked, handle_arakoon_errors, PyrakoonClient, PyrakoonLock
from .client_pooled import PyrakoonClientPooled
from .mock import MockPyrakoonClient
# Backwards compatibility imports
from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import Sequence, ArakoonAssertionFailed, ArakoonClient, ArakoonClientConfig, \
    ArakoonGoingDown, ArakoonNotFound, ArakoonNodeNotMaster, ArakoonNoMaster, ArakoonNotConnected, \
    ArakoonSocketException, ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError, Consistency
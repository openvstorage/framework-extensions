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
OVS-extensionds decorator test class
"""

import time
import unittest
from ovs_extensions.generic.decorators import timeout, TimeoutError


class TestDecorators(unittest.TestCase):

    def test_timeout(self):
        self.assertRaises(TimeoutError, self._count_to)
        self.assertTrue(TimeoutError, self._count_to(n=2))

    @timeout(3)
    def _count_to(self, n=5):
        time.sleep(n)
        return True
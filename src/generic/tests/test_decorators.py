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

import os
import time
import unittest
from ovs_extensions.generic.decorators import timeout, TimeoutError


class TestDecorators(unittest.TestCase):

    def test_timeout(self):
        self.assertRaises(TimeoutError, self._count_to)
        self.assertTrue(TimeoutError, self._count_to(n=2))

    @timeout(3)
    def _count_to(self,  n=5):
        file_path = None
        file_name = 'ovs_extentions_test_decorators.txt'
        try:
            with open(file_name, 'w') as fh:
                file_path = os.path.realpath(fh.name)
                for i in range(n):
                    fh.write('{0}\n'.format(i))
                    time.sleep(1)
                return TestDecorators._validate_contents(file_name, n)
        finally:
            if file_path is not None:
                #os.remove(file_path)
                pass
    @staticmethod
    def _validate_contents(file_name, count):
        with open(file_name, 'r') as fh:
            if fh.read() == '\n'.join([str(i) for i in range(count)]).strip():
                return True
            else:
                return False



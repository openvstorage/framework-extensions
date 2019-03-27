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
Test the Pyrakoon wrapper
"""

import unittest
from ovs_extensions.db.arakoon.pyrakoon.client import MockPyrakoonClient


class TestPyrakoon(unittest.TestCase):

    @staticmethod
    def _next_key(key):
        # type: (str) -> str
        """
        Calculates the next key (to be used in range queries)
        :param key: Key to calucate of
        :type key: str
        :return: The next key
        :rtype: str
        """
        encoding = 'ascii'  # For future python 3 compatibility
        array = bytearray(str(key), encoding)
        for index in range(len(array) - 1, -1, -1):
            array[index] += 1
            if array[index] < 128:
                while array[-1] == 0:
                    array = array[:-1]
                return str(array.decode(encoding))
            array[index] = 0
        return '\xff'

    def test_next_prefix_normal(self):
        """
        Test the prefix logic for normal keys
        """
        for prefix in ['ovs', 'alba']:
            self.assertEqual(MockPyrakoonClient._next_prefix(prefix), self._next_key(prefix))

    def test_next_prefix_breaking(self):
        """
        Old prefix would break because of encoding
        """
        break_prefix = 'a\xff'
        with self.assertRaises(UnicodeDecodeError):
            self._next_key(break_prefix)
        self.assertEqual(MockPyrakoonClient._next_prefix(break_prefix), 'b\x00')

    def test_empty_prefix(self):
        """
        Prefixing an empty string does not make much sense
        """
        empty_prefix = ''
        self.assertEqual(self._next_key(empty_prefix), '\xff')
        with self.assertRaises(ValueError):
            MockPyrakoonClient._next_prefix(empty_prefix)

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
Test module for the SSHClient class
"""
import unittest
from ovs_extensions.generic.configuration import Configuration


class ConfigurationTest(unittest.TestCase):
    """
    Test Configuration functionality
    """

    def test_set(self):
        """
        Test setting all kinds of data in Configuration
        """
        type_data = [(int, '/fooint', 1, False),
                     (basestring, '/foostr', 'foo', True),
                     (dict, '/foodict', {'foo': 'bar'}, False)]
        for data_type, key, value, raw in type_data:
            Configuration.set(key, value, raw=raw)
            get_value = Configuration.get(key, raw=raw)
            self.assertIsInstance(get_value, data_type)
            self.assertEquals(get_value, value)

    def test_set_advanced(self):
        """
        Test the advanced sets.
        Examples:
        > Configuration.set('/foo', {'bar': 1})
        > print Configuration.get('/foo')
        < {u'bar': 1}
        > print Configuration.get('/foo|bar')
        < 1
        > Configuration.set('/bar|a.b', 'test')
        > print Configuration.get('/bar')
        < {u'a': {u'b': u'test'}}
        """
        type_data = [((dict, '/foodict', {'foo': 'bar'}, False), (basestring, '/foodict|foo', 'bar', False)),
                     # Further build on foodict
                     ((basestring, '/foodict|bar', 'foo', False), (basestring, '/foodict|bar', 'foo', False))]
        for set_data, get_data in type_data:
            data_type, key, value, raw = set_data
            Configuration.set(key, value, raw=raw)
            get_value = Configuration.get(key, raw=raw)
            self.assertIsInstance(get_value, data_type)
            self.assertEquals(get_value, value)

            data_type, key, value, raw = get_data
            get_value = Configuration.get(key)
            self.assertIsInstance(get_value, data_type)
            self.assertEquals(get_value, value)

    def test_key_ambiguity(self):
        type_data = [((bool, '/foo/bar', True, False), (bool, 'foo/bar', True, False)),
                     ((bool, 'foo/bar', True, False), (bool, '/foo/bar', True, False))]
        for set_data, get_data in type_data:
            data_type, key, value, raw = set_data
            Configuration.set(key, value, raw=raw)
            get_value = Configuration.get(key, raw=raw)
            self.assertIsInstance(get_value, data_type)
            self.assertEquals(get_value, value)

            data_type, key, value, raw = get_data
            get_value = Configuration.get(key)
            self.assertIsInstance(get_value, data_type)
            self.assertEquals(get_value, value)

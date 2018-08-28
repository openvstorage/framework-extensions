# -*- coding: UTF-8 -*-
#  Copyright (C) 2016 iNuron NV
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
Test module for the udev generator
"""
import unittest
from ovs_extensions.generic.udev import UDevGenerator
from ovs_extensions.generic.udev import UDevRule


class UDEVGeneratorTest(unittest.TestCase):
    rule1 = UDevRule('kernel1', 'name1')
    rule2 = UDevRule('kernel1', 'name1', bus='bus1')
    rule3 = UDevRule('kernel1', 'name1', symlink='symlink1')

    def test_udevrule(self):
        self.assertEquals(str(self.rule1), 'KERNEL=="kernel1", NAME="name1"')
        self.assertEquals(str(self.rule2), 'BUS=="bus1", KERNEL=="kernel1", NAME="name1"')
        self.assertEquals(str(self.rule3), 'KERNEL=="kernel1", NAME="name1", SYMLINK+="symlink1"')

    def test_udev_generator(self):
        generator = UDevGenerator('test')
        generator.add_rule(self.rule1)
        generator.add_rule(self.rule2)
        generator.add_rule(self.rule3)
        self.assertEquals(str(generator), 'KERNEL=="kernel1", NAME="name1"\n' \
                                 'BUS=="bus1", KERNEL=="kernel1", NAME="name1"\n' \
                                 'KERNEL=="kernel1", NAME="name1", SYMLINK+="symlink1"')
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
Test module for nbd manager
"""

import unittest


class NBDTest(unittest.TestCase):
    """
    Test NBD functionality
    """
    VDISK_NAME = 'vd1'
    VOL_URI = '{0}://{1}:{2}@{3}:{4}/{5}'
    IP = '127.0.0.1'
    PORT = '8080'

    def get_nbd_manager(self):
        """
        Method to be overruled by implemented test
        :return:
        """
        raise NotImplementedError()

    def _test_create(self):
        nbd_manager = self.get_nbd_manager()
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=5)
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VOL_URI.format('ssh', 'user', 'pwd', self.IP, self.PORT, self.VDISK_NAME))
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VOL_URI.format('tcp', 'user', 'pwd', '5', self.PORT, self.VDISK_NAME))
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VOL_URI.format('tcp', 'user', 'pwd', 5, self.PORT, self.VDISK_NAME))
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VOL_URI.format('tcp', 'user', 'pwd', self.IP, -1, self.VDISK_NAME))
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VOL_URI.format('tcp', 'user', 'pwd', self.IP, 'bruh', self.VDISK_NAME))
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VDISK_NAME, block_size=30*1024)
        with self.assertRaises(RuntimeError):
            nbd_manager.create_service(volume_uri=self.VDISK_NAME, block_size='str')

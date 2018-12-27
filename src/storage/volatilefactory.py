# Copyright (C) 2016 iNuron NV
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
Generic volatile factory.
"""
import os


class VolatileFactory(object):
    """
    The VolatileFactory will generate certain default clients.
    """
    store = None

    @classmethod
    def get_client(cls, client_type=None):
        """
        Returns a volatile storage client
        """
        if cls.store is None:
            if os.environ.get('RUNNING_UNITTESTS') == 'True':
                client_type = 'dummy'
            if client_type is None:
                client_type = cls._get_client_type()

            cls.store = None
            if client_type == 'memcache':
                from ovs_extensions.storage.volatile.memcachestore import MemcacheStore
                configuration = cls._get_store_info()
                cls.store = MemcacheStore(**configuration)
            if client_type == 'dummy':
                from ovs_extensions.storage.volatile.dummystore import DummyVolatileStore
                cls.store = DummyVolatileStore()

        if cls.store is None:
            raise RuntimeError('Invalid client_type specified')
        return cls.store

    @classmethod
    def _get_store_info(cls):
        raise NotImplementedError()

    @classmethod
    def _get_client_type(cls):
        raise NotImplementedError()
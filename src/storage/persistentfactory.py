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
Generic persistent factory.
"""
import os
from ovs_extensions.storage.persistent.dummystore import DummyPersistentStore
from ovs_extensions.storage.persistent.pyrakoonstore import PyrakoonStore


class PersistentFactory(object):
    """
    The PersistentFactory will generate certain default clients.
    """

    @classmethod
    def get_client(cls, client_type=None):
        """
        Returns a persistent storage client
        :param client_type: Type of store client
        """
        # @todo Transition to PyrakoonStore in all cases. Not doing it in this hotfix due to the required mocking of arakoonclusters
        if not hasattr(cls, 'store') or cls.store is None:
            if os.environ.get('RUNNING_UNITTESTS') == 'True':
                client_type = 'dummy'

            if client_type is None:
                client_type = cls._get_client_type()

            cls.store = None
            if client_type in ['pyrakoon', 'arakoon']:
                store_info = cls._get_store_info()
                cls.store = PyrakoonStore(**store_info)
            if client_type == 'dummy':
                from ovs_extensions.storage.persistent.dummystore import DummyPersistentStore
                cls.store = DummyPersistentStore()

        if cls.store is None:
            raise RuntimeError('Invalid client_type specified')
        return cls.store

    @classmethod
    def _get_store_info(cls):
        raise NotImplementedError()

    @classmethod
    def _get_client_type(cls):
        raise NotImplementedError()

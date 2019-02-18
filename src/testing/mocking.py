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
Mocking module
Contains function to ensure/disable mocking
"""

from ..constants import is_unittest_mode


def clear_stores():
    from ovs_extensions.storage.volatilefactory import VolatileFactory
    from ovs_extensions.storage.persistentfactory import PersistentFactory
    from ovs_extensions.services.servicefactory import ServiceFactory

    # Clear the stores. It will force to recompute
    VolatileFactory.store = None
    PersistentFactory.store = None
    ServiceFactory.manager = None


def mock_all(check_for_unittest_mode=True):
    # type: (bool) -> None
    """
    Ensure that all instances are using mocked items instead of the real deal
    :param check_for_unittest_mode: Check if the unittest environment variable was set before mocking
    :type check_for_unittest_mode: bool
    :rtype: NoneType
    """
    if check_for_unittest_mode and not is_unittest_mode():
        return

    from ovs_extensions.generic.sshclient import SSHClient
    from ovs_extensions.storage.volatilefactory import VolatileFactory
    from ovs_extensions.storage.persistentfactory import PersistentFactory
    from ovs_extensions.services.servicefactory import ServiceFactory

    SSHClient.enable_mock()

    # Clear the stores. It will force to fetch a new instance
    # @todo what about already cached instances
    VolatileFactory.store = None
    PersistentFactory.store = None
    ServiceFactory.manager = None


def disable_mock(check_for_unittest_mode=True):
    # type: (bool) -> None
    """
    Disable all mocking. Using the real deal again
    :param check_for_unittest_mode: Check if the unittest environment variable was set before disabling mocking
    :type check_for_unittest_mode: bool
    :return:
    """

    if check_for_unittest_mode and is_unittest_mode():
        return

    from ovs_extensions.generic.sshclient import SSHClient
    from ovs_extensions.storage.volatilefactory import VolatileFactory
    from ovs_extensions.storage.persistentfactory import PersistentFactory
    from ovs_extensions.services.servicefactory import ServiceFactory

    SSHClient.disable_mock()

    # Clear the stores. It will force to fetch a new instance
    # @todo what about already cached instances
    VolatileFactory.store = None
    PersistentFactory.store = None
    ServiceFactory.manager = None
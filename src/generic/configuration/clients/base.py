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
Dummy persistent module
"""
from ovs_extensions.log.logger import Logger


class ConfigurationClientBase(object):
    """
    Base for all ConfigurationClients.
    These are built and used by the Configuration abstraction.
    All inheriting classes must overrule lock, get_configuration_path, extract_key_from_path, get, set, dir_exists, list, delete, rename
    """

    _logger = Logger('extensions')

    def __init__(self, *args, **kwargs):
        _ = args, kwargs

    def lock(self, name, wait=None, expiration=60):
        raise NotImplementedError()

    def dir_exists(self, key):
        # type: (str) -> bool
        raise NotImplementedError()

    def list(self, key, recursive):
        # type: (str, bool) -> Iterable(str)
        raise NotImplementedError()

    def delete(self, key, recursive):
        # type: (str, bool) -> None
        raise NotImplementedError()

    def get(self, key, **kwargs):
        # type: (str, **kwargs) -> Union[dict, None]
        raise NotImplementedError()

    def get_client(self):
        raise NotImplementedError()

    def set(self, key, value):
        # type: (str, any) -> None
        raise NotImplementedError()

    def rename(self, key, new_key, max_retries):
        # type: (str, str, int) -> None
        raise NotImplementedError()

    @classmethod
    def extract_key_from_path(cls, path):
        # type: (str) -> str
        raise NotImplementedError()

    @classmethod
    def get_configuration_path(cls, key):
        # type: (str) -> str
        raise NotImplementedError()

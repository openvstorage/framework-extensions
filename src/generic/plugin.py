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
Plugincontroller parent class module
"""

import os
import inspect
import importlib
from ovs_extensions.constants.modules import BASE_OVS, SOURCE_DAL_HYBRIDS


class PluginController():
    """
    Base controller class to offload moduleimports for specific plugins
    """

    @classmethod
    def get_hybrids(cls, source_folder=BASE_OVS):
        # type: (Optional[str]) -> List[str]  #todo sourcefolder might need formatting to module path instead of filepath
        return [c for c in cls._fetch_classes(SOURCE_DAL_HYBRIDS.format(source_folder)) if 'Base' in c[1].__name__]


    @classmethod
    def _fetch_classes(cls, path):
        # type: (str) -> List[tuple(str, str)]
        classes = []
        major_mod = importlib.import_module(path)
        for filename in os.listdir(major_mod.__path__[0]):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py') and filename != '__init__.py':
                name = '.'.join([path, filename.replace('.py', '')])
                mod = importlib.import_module(name)
                for member in inspect.getmembers(mod, predicate=inspect.isclass):
                    if member[1].__module__ == name:
                        classes.append(member)
        return classes

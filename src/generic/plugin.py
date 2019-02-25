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
from ovs_extensions.constants.file_extensions import PY
from ovs_extensions.constants.modules import SOURCE_DAL_OBJECTS


class PluginController():
    """
    Base controller class to offload moduleimports for specific plugins
    """

    @classmethod
    def get_hybrids(cls, source_folder):
        # type: (Optional[str]) -> List[str]

        """
        Fetch the hybrids module in the wanted source folder. This is either ovs core or one of the plugins
        :param source_folder: folder to fetch hybrids from. Defaults to ovs core
        :return: list with hybrids
        """
        # todo sourcefolder might need formatting to module path instead of filepath
        return [c for c in cls._fetch_classes(SOURCE_DAL_OBJECTS.format(source_folder)) if 'Base' in c[1].__name__]


    @classmethod
    def _fetch_classes(cls, path):
        # type: (str) -> List[tuple(str, str)]
        classes = []
        major_mod = importlib.import_module(path)
        filepath = major_mod.__path__[0]
        for filename in os.listdir(filepath):
            if os.path.isfile('/'.join([filepath, filename])) and filename.endswith(PY) and filename != '__init__.py':
                name = filename.replace(PY, '')
                mod_path = '.'.join([path, name])
                mod = importlib.import_module(mod_path)
                for member_name, member in inspect.getmembers(mod, predicate=inspect.isclass):
                    if member.__module__ == mod_path:
                        classes.append(member)
        return classes

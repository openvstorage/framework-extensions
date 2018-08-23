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
UDev Generator
"""

from collections import OrderedDict


class UDevGenerator(object):

    base_path = '/lib/udev/rules.d/{0}'

    def __init__(self, name, level=99):
        # type(str, Optional[int]) -> None
        """
        Generates a Udev generator object, that can be expanded with various seperate rules.
        :param name: Name that will be
        :param level:
        """
        self.name = name
        self.level = level
        self.content = []

    def add_rule(self, rule):
        # type(UDevRule) -> None
        if not isinstance(rule, UDevRule):
            raise RuntimeError('Illegal argument: rule must be of type {0}'.format(UDevRule))
        self.content.append(rule)

    def make_name(self):
        return '{0}-{1}.rule'.format(self.level, self.name)

    def write_content(self, path=base_path):
        path = path.format(self.make_name())
        with open(path) as fh:
            fh.write(str(self))

    def __str__(self):
        return '\n'.join([str(i) for i in self.content])  # Stringify every rule instance


class UDevRule():
    operatordict = OrderedDict(
        [('bus', '=='),
         ('kernel', '=='),
         ('subsystem', '=='),
         ('name', '='),
         ('symlink', '+=')]
    )

    def __init__(self, kernel, name, bus=None, subsystem=None, symlink=None):
        self.bus = bus
        self.kernel = kernel
        self.name = name
        self.subsystem=subsystem
        self.symlink = symlink

    def __str__(self):
        out = []
        for key, value in self.operatordict.items():
            attribute_value = getattr(self, key)
            if attribute_value:
                out.append('{0}{1}"{2}"'.format(key.upper(), self.operatordict.get(key), attribute_value))
        return ', '.join(out)

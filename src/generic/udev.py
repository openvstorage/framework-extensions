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
        # type: (str, Optional[int]) -> None
        """
        Generates a Udev generator object, that can be expanded with various seperate rules.
        :param name: Name that will be used for the udev rule.
        :param level: Level of order assigned to the udev rule. Rule 1 will be assigned first, 99 last.
        """
        self.name = name
        self.level = level
        self.content = []

    def add_rule(self, rule):
        # type: (UDevRule) -> None
        """
        Add a UDevRule to the content of this object.
        :param rule: UDevRule object
        :return: None
        """
        if not isinstance(rule, UDevRule):
            raise RuntimeError('Illegal argument: rule must be of type {0}'.format(UDevRule))
        self.content.append(rule)

    def make_name(self):
        # type: (None) -> str
        return '{0}-{1}.rule'.format(self.level, self.name)

    def write_content(self, path=base_path):
        # type: (Optional[str]) -> None
        """
        Will write the content attribute of the UDevGenerator object to the udev rule file with UDevGenerator.make_name
        :param path: the base path of the udev rule folder
        :return: None
        """
        path = path.format(self.make_name())
        with open(path, 'w') as fh:
            fh.write(str(self))

    def __str__(self):
        return '\n'.join([str(i) for i in self.content])  # Stringify every rule instance


class UDevRule(object):
    operatordict = [('bus', '=='),
         ('kernel', '=='),
         ('subsystem', '=='),
         ('name', '='),
         ('symlink', '+=')]

    def __init__(self, kernel, name, bus=None, subsystem=None, symlink=None):
        # type: (str, str, Optional[str], Optional[str], Optional[str]) -> None
        """
        The parameters of the UDevRule object reflect the fields that are allowed in udev rules.
        str(UDevRule) will return a string with a correctly formatted udev rule.
        :param kernel: Kernel of the device of interest
        :param name: Name that will be given to the device
        :param bus: Bus of the device of interest
        :param subsystem: Subsystem of the device of interest
        :param symlink: Symlink that will be given to the device
        """
        self.bus = bus
        self.kernel = kernel
        self.name = name
        self.subsystem=subsystem
        self.symlink = symlink

    def __str__(self):
        out = []
        for key, value in self.operatordict:
            attribute_value = getattr(self, key)
            if attribute_value:
                out.append('{0}{1}"{2}"'.format(key.upper(), value, attribute_value))
        return ', '.join(out)

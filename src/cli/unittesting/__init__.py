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

from __future__ import absolute_import

import click
from ..commands import OVSCommand


def unittest_command_unwrapped(action, averages):
    from ovs_extensions.generic.unittests import UnitTest

    ut = UnitTest('ovs')
    if not action:
        ut.run_tests(add_averages=averages)
    elif action == 'list':
        ut.list_tests(print_tests=True)
    else:
        action = str(action)

        if action.endswith('.py'):
            filename = action.rstrip('.py')
        else:
            filename = action
        ut.run_tests(filename, add_averages=averages)


@click.command('unittest', help='Run all or a part of the OVS unittest suite', section_header='Unittest', cls=OVSCommand)
@click.argument('action', required=False, default=None, type=click.STRING)
@click.option('--averages', is_flag=True, default=False)
def unittest_command(action, averages):
    return unittest_command_unwrapped(action, averages)

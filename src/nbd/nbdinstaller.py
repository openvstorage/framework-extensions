#!/usr/bin/python2

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
Module for iSCSI Manager SetupController
"""
import argparse
from subprocess import check_output

NBDS_MAX_DEFAULT = 255
MAX_PART_DEFAULT = 15
NBD_MODPROBE_LOCATION = '/etc/modprobe.d/nbd.conf'
MODULES_PATH = '/etc/modules'


class NBDInstaller:

    @staticmethod
    def setup(*args, **kwargs):
        """
        Setup of the nbd manager
        :param nbds_max:
        :type nbds_max: int
        :param max_part: maximum number of partitions to be made
        :type max_part: int
        :return:
        """
        print 'Started setup of NBD-manager.'

        #  todo check if module is loaded

        with open(NBD_MODPROBE_LOCATION, 'w') as fh:
            for k, v in kwargs:
                print k, v
                fh.write('options nbd {0}={1}\n'.format(k, v))
            # fh.write('options nbd max_part={0}\n'.format(max_part))
        with open(MODULES_PATH, 'r+') as fh2:
            fh2.write('volumedriver-nbd') #  todo correct?

        check_output(['modprobe', 'nbd'])
        check_output(['apt-get', 'install', 'volumedriver-nbd'])

        print 'Succesfully loaded NBD'

    @staticmethod
    def remove(*args):
        """
        :return:
        """
        print 'Started removal of NBD-manager.'
        check_output(['rm', NBD_MODPROBE_LOCATION])
        file = open(MODULES_PATH, 'r')
        lines = file.readlines()
        file.close()
        with open(MODULES_PATH, 'w') as fh:
            for line in lines:
                if 'volumedriver-nbd' in line:
                    continue
                else:
                    fh.write(line)
        check_output(['apt-get', 'remove', 'volumedriver-nbd']) #todo nodig?
        print 'Succesfully removed NBD'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='nbd-manager', description='NBD_MANAGER USAGE')
    subparsers = parser.add_subparsers(help='possible flows for the NBD manager')
    parser_setup = subparsers.add_parser('setup', help='Run NBD {0} '.format('setup'))
    parser_setup.add_argument('--nbds_max', help="the maximal number of nbd volumes per node to be made. Defaults to {0}".format(NBDS_MAX_DEFAULT), type=int, default=NBDS_MAX_DEFAULT)
    parser_setup.add_argument('--max_part', help="the maximal number of partitions to be made. Defaults to {0}".format(MAX_PART_DEFAULT), type=int, default=MAX_PART_DEFAULT)
    parser_setup.set_defaults(func=NBDInstaller.setup)

    parser_remove = subparsers.add_parser('remove', help='Run NBD {0} '.format('flow'))
    parser_remove.set_defaults(func=NBDInstaller.remove)

    arguments = parser.parse_args()
    arguments.func(arguments)

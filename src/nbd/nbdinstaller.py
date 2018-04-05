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
from ovs_extensions.log.logger import Logger


class NBDInstaller:
    """
    Command line NBD installer
    """
    _logger = Logger('extensions-nbd')

    def __init__(self):
        pass

    NBDS_MAX_DEFAULT = 255
    MAX_PART_DEFAULT = 15
    NBD_MODPROBE_LOCATION = '/etc/modprobe.d/nbd.conf'
    MODULES_PATH = '/etc/modules'

    @staticmethod
    def setup(nbds_max=NBDS_MAX_DEFAULT, max_part=MAX_PART_DEFAULT):
        # type: (int, int) -> None
        """
        Setup of the nbd manager. This visible function should be used for installing via python shell and only accepts correct arguments.
        Only allowed parameters are currently
        :param nbds_max:
        :type nbds_max: int
        :param max_part: maximum number of partitions to be made
        :type max_part: int
        :return: None
        """
        NBDInstaller._setup(nbds_max=nbds_max, max_part=max_part)

    @staticmethod
    def _setup(**kwargs):
        # type: (any) -> None
        """
        Setup of the nbd manager. Only allowed parameters are currently
        :param nbds_max:
        :type nbds_max: int
        :param max_part: maximum number of partitions to be made
        :type max_part: int
        :return: None
        """
        NBDInstaller._logger.info('Started setup of NBD-manager.')
        with open(NBDInstaller.NBD_MODPROBE_LOCATION, 'w') as fh:
            for k, v in kwargs.iteritems():
                fh.write('options nbd {0}={1}\n'.format(k, v))
        with open(NBDInstaller.MODULES_PATH, 'r+') as fh2:
            fh2.write('volumedriver-nbd')

        check_output(['modprobe', 'nbd'])
        check_output(['apt-get', 'install', 'volumedriver-nbd'])

        NBDInstaller._logger.info('Succesfully loaded NBD')

    @staticmethod
    def remove():
        """
        Removes the NBD manager
        :return: None
        """
        # type: None -> None
        NBDInstaller._logger.info('Started removal of NBD-manager.')
        check_output(['rm', NBDInstaller.NBD_MODPROBE_LOCATION])
        module_file = open(NBDInstaller.MODULES_PATH, 'r')
        lines = module_file.readlines()
        module_file.close()
        with open(NBDInstaller.MODULES_PATH, 'w') as fh:
            for line in lines:
                if 'volumedriver-nbd' in line:
                    continue
                else:
                    fh.write(line)
        check_output(['apt-get', 'remove', 'volumedriver-nbd'])
        NBDInstaller._logger.info('Succesfully removed NBD')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='nbd-manager', description='NBD_MANAGER USAGE')
    subparsers = parser.add_subparsers(help='possible flows for the NBD manager')
    parser_setup = subparsers.add_parser('setup', help='Run NBD {0} '.format('setup'))
    parser_setup.add_argument('--nbds_max', help="the maximal number of nbd volumes per node to be made. Defaults to {0}".format(NBDInstaller.NBDS_MAX_DEFAULT), type=int, default=NBDInstaller.NBDS_MAX_DEFAULT)
    parser_setup.add_argument('--max_part', help="the maximal number of partitions to be made. Defaults to {0}".format(NBDInstaller.MAX_PART_DEFAULT), type=int, default=NBDInstaller.MAX_PART_DEFAULT)
    parser_setup.set_defaults(func=NBDInstaller._setup)

    parser_remove = subparsers.add_parser('remove', help='Run NBD {0} '.format('flow'))
    parser_remove.set_defaults(func=NBDInstaller.remove)

    arguments = parser.parse_args()
    arguments.func(arguments)

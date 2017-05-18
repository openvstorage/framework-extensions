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
Package Factory module
"""
import os
from subprocess import check_output
from ovs_extensions.packages.interfaces.debian import DebianPackage
from ovs_extensions.packages.interfaces.rpm import RpmPackage


class PackageFactory(object):
    """
    Factory class returning specialized classes
    """

    @classmethod
    def get_manager(cls):
        """
        Returns a package manager
        """
        if not hasattr(PackageFactory, 'manager') or PackageFactory.manager is None:
            distributor = None
            check_lsb = check_output('which lsb_release 2>&1 || true', shell=True).strip()
            if "no lsb_release in" in check_lsb:
                if os.path.exists('/etc/centos-release'):
                    distributor = 'CentOS'
            else:
                distributor = check_output('lsb_release -i', shell=True)
                distributor = distributor.replace('Distributor ID:', '').strip()

            if distributor in ['Ubuntu']:
                PackageFactory.manager = DebianPackage
            elif distributor in ['CentOS']:
                PackageFactory.manager = RpmPackage

        if PackageFactory.manager is None:
            raise RuntimeError('Unknown PackageManager')

        return PackageFactory.manager

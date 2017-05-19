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
Service Factory module
"""
import os
import logging
from subprocess import check_output
from ovs_extensions.services.interfaces.systemd import Systemd
from ovs_extensions.services.interfaces.upstart import Upstart
from ovs_extensions.services.mockups.systemd import SystemdMock

logger = logging.getLogger(__name__)


class ServiceFactory(object):
    """
    Factory class returning specialized classes
    """

    @classmethod
    def get_service_type(cls):
        """
        Gets the service manager type
        """
        try:
            init_info = check_output('cat /proc/1/comm', shell=True)
            if 'init' in init_info:
                version_info = check_output('init --version', shell=True)
                if 'upstart' in version_info:
                    return 'upstart'
            elif 'systemd' in init_info:
                return 'systemd'
        except Exception as ex:
            logger.exception('Error loading ServiceManager: {0}'.format(ex))
            raise
        return None

    @classmethod
    def get_manager(cls):
        """
        Returns a service manager
        """
        if not hasattr(ServiceFactory, 'manager') or ServiceFactory.manager is None:
            if os.environ.get('RUNNING_UNITTESTS') == 'True':
                ServiceFactory.manager = SystemdMock
            else:
                service_type = cls.get_service_type()
                if service_type == 'upstart':
                    ServiceFactory.manager = Upstart
                elif service_type == 'systemd':
                    ServiceFactory.manager = Systemd

        if ServiceFactory.manager is None:
            raise RuntimeError('Unknown ServiceManager')

        ServiceFactory.manager.RUN_FILE_DIR = cls.RUN_FILE_DIR
        ServiceFactory.manager.CONFIG_TEMPLATE_DIR = cls.CONFIG_TEMPLATE_DIR
        return ServiceFactory.manager

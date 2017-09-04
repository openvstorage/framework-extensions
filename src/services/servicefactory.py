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
from subprocess import check_output
from ovs_extensions.services.interfaces.systemd import Systemd
from ovs_extensions.services.interfaces.upstart import Upstart
from ovs_extensions.services.mockups.systemd import SystemdMock


class ServiceFactory(object):
    """
    Factory class returning specialized classes
    """
    RUN_FILE_DIR = None
    MONITOR_PREFIXES = None
    SERVICE_CONFIG_KEY = None
    CONFIG_TEMPLATE_DIR = None

    @classmethod
    def get_service_type(cls):
        """
        Gets the service manager type
        """
        init_info = check_output('cat /proc/1/comm', shell=True)
        if 'init' in init_info:
            version_info = check_output('init --version', shell=True)
            if 'upstart' in version_info:
                return 'upstart'
        elif 'systemd' in init_info:
            return 'systemd'
        return None

    @classmethod
    def get_manager(cls):
        """
        Returns a service manager
        """
        if not hasattr(cls, 'manager') or cls.manager is None:
            implementation_class = None
            if os.environ.get('RUNNING_UNITTESTS') == 'True':
                implementation_class = SystemdMock
            else:
                service_type = cls.get_service_type()
                if service_type == 'upstart':
                    implementation_class = Upstart
                elif service_type == 'systemd':
                    implementation_class = Systemd
            if implementation_class is not None:
                cls.manager = implementation_class(system=cls._get_system(),
                                                   logger=cls._get_logger_instance(),
                                                   configuration=cls._get_configuration(),
                                                   run_file_dir=cls.RUN_FILE_DIR,
                                                   monitor_prefixes=cls.MONITOR_PREFIXES,
                                                   service_config_key=cls.SERVICE_CONFIG_KEY,
                                                   config_template_dir=cls.CONFIG_TEMPLATE_DIR)

        if cls.manager is None:
            raise RuntimeError('Unknown ServiceManager')
        return cls.manager

    @classmethod
    def _get_system(cls):
        raise NotImplementedError()

    @classmethod
    def _get_configuration(cls):
        raise NotImplementedError()

    @classmethod
    def _get_logger_instance(cls):
        raise NotImplementedError()

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
from .interfaces.base import ServiceAbstract
from .interfaces.systemd import Systemd
from .interfaces.upstart import Upstart
from .mockups.systemd import SystemdMock
from ovs_extensions.log.logger import Logger


INIT = 'init'
UPSTART = 'upstart'
SYSTEMD = 'systemd'
MOCK = 'mock'


class ServiceFactory(object):
    """
    Factory class returning specialized classes
    """
    # Singleton holder
    manager = None

    RUN_FILE_DIR = None
    MONITOR_PREFIXES = None
    SERVICE_CONFIG_KEY = None
    CONFIG_TEMPLATE_DIR = None

    TYPE_IMPLEMENTATION_MAP = {UPSTART: Upstart,
                               SYSTEMD: Systemd,
                               MOCK: SystemdMock}

    @classmethod
    def get_service_type(cls):
        # type: () -> str
        """
        Gets the service manager type
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return MOCK

        init_info = check_output(['cat', '/proc/1/comm'])
        if INIT in init_info:
            version_info = check_output(['init', '--version'])
            if UPSTART in version_info:
                return UPSTART
        elif SYSTEMD in init_info:
            return SYSTEMD
        raise EnvironmentError('Unable to determine service management type')

    @classmethod
    def get_manager(cls):
        # type: () -> ServiceAbstract
        """
        Returns a service manager
        """
        if cls.manager is None:
            service_type = cls.get_service_type()
            implementation_class = cls.TYPE_IMPLEMENTATION_MAP.get(service_type)
            if implementation_class:
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
        return Logger('extensions-services')

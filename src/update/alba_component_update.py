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

import re
import os
import time
import logging
from distutils.version import LooseVersion
from subprocess import check_output, CalledProcessError
from ovs_extensions.storage.persistent.pyrakoonstore import PyrakoonStore
from ovs_extensions.services.interfaces.systemd import SystemdUnitParser
from ovs_extensions.update.base import ComponentUpdater
from ovs_extensions.db.arakoon.arakooninstaller import ArakoonClusterConfig
from StringIO import StringIO

logger = logging.getLogger(__name__)


class NoMasterFoundException(EnvironmentError):
    """
    Raise this error when no arakoon master can be found after a couple of attempts
    """

class InvalidAlbaVersionException(EnvironmentError):
    """
    Will be called if no valid alba version has been found and the update-alternatives call has failed with this alba version
    """


class AlbaComponentUpdater(ComponentUpdater):
    """
    Implementation of abstract class to update alba
    """

    COMPONENT = 'alba'
    BINARIES = [('alba-ee', 'alba', '/usr/bin/alba')]  # List with tuples. [(package_name, binary_name, binary_location, [service_prefix_0]]

    alba_binary_base_path = os.path.join(os.path.sep, 'opt')

    re_abm = re.compile('^ovs-arakoon.*-abm$')
    re_nsm = re.compile('^ovs-arakoon.*-nsm_[0-9]*$')
    re_alba_proxy = re.compile('^ovs-albaproxy_.*$')
    re_alba_binary = re.compile('^alba-[0-9.a-z]*$')
    re_alba_asd = re.compile('^alba-asd-[0-9a-zA-Z]{32}$')
    re_exec_start = re.compile('.* -config (?P<config>\S*) .*')
    re_alba_maintenance = re.compile('^alba-maintenance_.*-[0-9a-zA-Z]{16}$')

    @staticmethod
    def get_persistent_client():
        # type: () -> PyrakoonStore
        """
        Retrieve a persistent client which needs
        Needs to be implemented by the callee
        """
        raise NotImplementedError()

    @classmethod
    def get_node_id(cls):
        # type: () -> str
        raise NotImplementedError()

    @classmethod
    def update_alternatives(cls):
        # type: () -> None
        """
        update the /etc/alternatives alba symlink to the most recent alba version.
        the alba in /usr/bin/alba is a symlink to file in /etc/alternatives, as set by ops or this function.
        the /etc/alternatives files are in turn a symlink to /opt/alba-*.
        The alternatives are used to be able to run multiple alba instances on the same node

        :return: None
        """
        try:
            version = max([LooseVersion(i) for i in os.listdir(cls.alba_binary_base_path) if cls.re_alba_binary.match(i)])
            try:
                check_output(['update-alternatives', '--set', 'alba', os.path.join(os.path.sep, cls.alba_binary_base_path, str(version))])
            except CalledProcessError:
                raise InvalidAlbaVersionException('Invalid alba version has been found and used for updating alternatives: {0}'.format(version or 'None'))
        except ValueError:
            raise RuntimeError('No valid alba binaries have been found in {0}. Not updating alternatives.'.format(cls.alba_binary_base_path))

    @classmethod
    def update_binaries(cls):
        # type: () -> None
        """
        Update the binary
        :return:
        """
        if cls.BINARIES is None:
            raise NotImplementedError('Unable to update packages. Binaries are not included')
        for package_name, _, _ in cls.BINARIES:
            logging.info('Updating {}'.format(package_name))
            cls.install_package(package_name)
            cls.update_alternatives()

    @classmethod
    def get_arakoon_config_url(cls, service):
        # type: (str) -> str
        """
        Fetches the local file path of a given arakoon service, and parses the execstart configfile location
        :param service: ovs-arakoon.*
        :return: config file location
        """
        local_client = cls.get_local_root_client()

        file_path = cls.SERVICE_MANAGER.get_service_file_path(service)
        file_contents = local_client.file_read(file_path)
        parser = SystemdUnitParser()
        parser.readfp(StringIO(file_contents))
        try:
            execstart_rule = parser.get('Service', 'ExecStart')
            return cls.re_exec_start.search(execstart_rule).groupdict().get('config')
        except:
            raise RuntimeError('This execStart rule did not contain an execStart rule')

    @classmethod
    def restart_services(cls):
        # type: () -> None
        """
        Restart related services
        :return:
        """
        local_client = cls.get_local_root_client()
        node_id = cls.get_node_id()
        all_services = cls.SERVICE_MANAGER.list_services(local_client)

        # restart arakoons first, drop master if this node is master
        arakoon_services = [i for i in all_services if i.startswith('ovs-arakoon')]
        for service in arakoon_services:
            arakoon_config_url = cls.get_arakoon_config_url(service)
            if node_id == cls.get_arakoon_master(arakoon_config_url):
                cls.drop_arakoon_master(arakoon_config_url)
            master_node = cls.get_arakoon_master(arakoon_config_url)
            if master_node:
                cls.SERVICE_MANAGER.restart_service(service, local_client)

        # restart other alba related services after making sure arakoons are ok
        maintenance_services = [i for i in all_services if cls.re_alba_maintenance.match(i)]
        for service in maintenance_services:
            cls.SERVICE_MANAGER.restart_service(service, local_client)
        abm_nsm_services = [i for i in all_services if cls.re_nsm.match(i) or cls.re_abm.match(i)]
        for service in abm_nsm_services:
            cls.SERVICE_MANAGER.restart_service(service, local_client)
        asd_services = [i for i in all_services if cls.re_alba_asd.match(i)]
        for service in asd_services:
            cls.SERVICE_MANAGER.restart_service(service, local_client)
        proxy_services = [i for i in all_services if cls.re_alba_proxy.match(i)]
        for service in proxy_services:
            cls.SERVICE_MANAGER.restart_service(service, local_client)

    @staticmethod
    def get_arakoon_master(arakoon_config_url, max_attempts=10):
        # type: (str) -> str
        """
        Fetches the master node id, based on the arakoon config url
        :param arakoon_config_url: str
        :param max_attempts: int
        :return: str
        """
        attempt = 0
        while True:
            try:
                return check_output(['arakoon', '--who-master', '-config', arakoon_config_url])
            except CalledProcessError:
                attempt += 1

            if max_attempts == attempt:
                raise NoMasterFoundException("Couldn't find arakoon master node after {0} attempts".format(max_attempts))

            time.sleep(5)

    @classmethod
    def drop_arakoon_master(cls, config_url):
        # type: (str) -> str
        """
        Drops this node as arakoon master node, for safe updating
        :return: str
        """
        node_id = cls.get_node_id()
        # We don't really need any automation. It's just for casting into an object
        cluster_config = ArakoonClusterConfig(None)
        cluster_config.read_config(contents=cls.read_arakoon_config(config_url))
        for node in cluster_config.nodes:
            if node.name == node_id:
                return check_output(['arakoon', '--drop-master', node_id, '127.0.0.1', node.port])

        raise RuntimeError('No arakoon node ID matched the local machine ID, no arakoon master have been dropped.')


    @classmethod
    def read_arakoon_config(cls, config_url):
        # type: (str) -> str
        if config_url.startswith('arakoon://'):
            content = check_output(['alba', 'dev-extract-config', '--config', config_url])
        else:
            with open(config_url, 'r') as fh:
                content = fh.read()
        return content


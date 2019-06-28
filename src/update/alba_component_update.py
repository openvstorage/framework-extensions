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
from itertools import chain
from distutils.version import LooseVersion
from subprocess import check_output, CalledProcessError
from ovs_extensions.services.interfaces.systemd import SystemdUnitParser
from ovs_extensions.update.base import ComponentUpdater, UpdateException
from ovs_extensions.db.arakoon.arakooninstaller import ArakoonClusterConfig
from StringIO import StringIO


class NoMasterFoundException(UpdateException):
    """
    Raise this error when no arakoon master can be found after a couple of ComponentUpdaterattempts
    """
    error_code = 61


class InvalidAlbaVersionException(UpdateException):
    """
    Will be called if no valid alba version has been found and the update-alternatives call has failed with this alba version
    """
    error_code = 62


class AlbaComponentUpdater(ComponentUpdater):
    """
    Implementation of abstract class to update alba
    """

    COMPONENT = 'alba'
    BINARIES = [(['alba'], 'alba', '/usr/bin/alba', [])]  # List with tuples. [(package_name, binary_name, binary_location, [service_prefix_0]]

    ## Regexes
    re_abm = re.compile('^ovs-arakoon.*-abm$')
    re_nsm = re.compile('^ovs-arakoon.*-nsm_[0-9]*$')
    re_alba_proxy = re.compile('^ovs-albaproxy_.*$')
    re_alba_binary = re.compile('^alba-[0-9.a-z]*$')
    re_alba_binary_version = re.compile('alba(-|-..\.)?(?P<version>[0-9.a-z]*)$')
    re_alba_asd = re.compile('^alba-asd-[0-9a-zA-Z]{32}$')
    re_exec_start = re.compile('.* -config (?P<config>\S*) .*')
    re_alba_maintenance = re.compile('^alba-maintenance_.*-[0-9a-zA-Z]{16}$')

    PRIORITY = str(42)

    ## Pathing variables
    OPT = os.path.join(os.path.sep, 'opt')
    ALBA_OPT_PATH = os.path.join(OPT, '{0}')
    ALBA_OPT_ALBA_BINARY_PATH = os.path.join(os.path.join(ALBA_OPT_PATH, 'bin', 'alba'))
    ALBA_OPT_ARAKOON_BINARY_PATH = os.path.join(ALBA_OPT_PATH, 'bin', 'arakoon')
    ALBA_OPT_ABM_PLUGIN_PATH = os.path.join(ALBA_OPT_PATH, 'plugin', 'albamgr_plugin.cmxs')
    ALBA_OPT_NSM_PLUGIN_PATH = os.path.join(ALBA_OPT_PATH, 'plugin', 'nsm_host_plugin.cmxs')

    USR = os.path.join(os.path.sep, 'usr')
    ALBA_BIN_PATH = os.path.join(USR, 'bin', 'alba')
    ARAKOON_BIN_PATH = os.path.join(USR, 'bin', 'arakoon')

    ALBA_LIB_PATH = os.path.join(USR, 'lib', 'alba')
    ALBA_LIB_ABM_PLUGIN_PATH = os.path.join(ALBA_LIB_PATH, 'albamgr_plugin.cmxs')
    ALBA_LIB_NSM_PLUGIN_PATH = os.path.join(ALBA_LIB_PATH, 'nsm_host_plugin.cmxs')

    logger = logging.getLogger(__name__)

    @classmethod
    def get_node_id(cls):
        """
        Fetch the local id. should be implemented by ovs or the asd manager
        :return: The local node id
        """
        # type: () -> str
        raise NotImplementedError()

    @classmethod
    def update_alternatives(cls):
        # type: () -> None
        """
        Update all required links regarding changes from andes update 3 towards andes updates 4.
        Includes:
        - usage of /etc/alternatives
        - arakoon packages to match the alba package
        - update the plugin symlinks

        mimics https://github.com/openvstorage/alba_ee/blob/master-ee/for_debian/alba-ee.postinst
        :return: None
        """
        cls.logger.info('Updating alternatives')
        alba_versions = cls.get_local_alba_versions()
        latest_alba_version = [(k, v) for k, v in alba_versions.iteritems() if v == max(alba_versions.values())][0]
        if os.path.exists(cls.ALBA_BIN_PATH) and not os.path.islink(cls.ALBA_BIN_PATH):
            cls.update_alba_alternatives(latest_alba_version)
        if os.path.exists(cls.ALBA_BIN_PATH) and not os.path.islink(cls.ARAKOON_BIN_PATH):
            cls.update_arakoon_links(latest_alba_version)

    @classmethod
    def update_arakoon_links(cls, latest_alba_version):
        """
        Arakoon binary needs to be updated to latest greatest, as present in the install folder of the alba package.
        :param latest_alba_version: tuple containing albaversion and parsed version comparison eg. (alba-ee.1.5.35, 1.5.35)
        :return: None
        """
        cls.logger.info('Updating arakoon alternatives')
        # dpkg-divert will give arakoon ownership on arakoon.old instead of arakoon
        check_output(['dpkg-divert', '--package', str(latest_alba_version[0]), '--divert', cls._to_old(cls.ARAKOON_BIN_PATH), '--rename', cls.ARAKOON_BIN_PATH])
        arakoon_old_binary_path_exists = os.path.exists(cls._to_old(cls.ARAKOON_BIN_PATH))
        if arakoon_old_binary_path_exists:
            check_output(['update-alternatives', '--install', cls.ARAKOON_BIN_PATH, 'arakoon', cls._to_old(cls.ARAKOON_BIN_PATH), cls.PRIORITY])
        # install new arakoon
        check_output(['update-alternatives', '--install', cls.ARAKOON_BIN_PATH, 'arakoon', cls.ALBA_OPT_ARAKOON_BINARY_PATH.format(latest_alba_version[0]), cls.PRIORITY])
        # update links (/usr/bin/arakoon will be discarded
        check_output(['update-alternatives', '--auto', 'arakoon'])
        # place link to /opt/alba.x/bin/arakoon
        check_output(['update-alternatives', '--auto', 'arakoon'])



    @classmethod
    def update_alba_alternatives(cls, latest_alba_version):
        """
        # Use for alba-ee <= 1.5.28

        Update the /etc/alternatives alba symlink to the most recent alba version.
        the alba in /usr/bin/alba is a symlink to file in /etc/alternatives, as set by ops or this function.
        the /etc/alternatives files are in turn a symlink to /opt/alba-*.
        The alternatives are used to be able to run multiple alba instances on the same node
        :return:
        """
        cls.logger.info('Updating alba alternatives')

        try:
            check_output(['dpkg-divert', '--package', latest_alba_version[0], '--divert', cls._to_old(cls.ALBA_BIN_PATH), '--rename', cls.ALBA_BIN_PATH])
            check_output(['dpkg-divert', '--package', latest_alba_version[0], '--divert', cls._to_old(cls.ALBA_LIB_ABM_PLUGIN_PATH), '--rename', cls.ALBA_LIB_ABM_PLUGIN_PATH])
            check_output(['dpkg-divert', '--package', latest_alba_version[0], '--divert', cls._to_old(cls.ALBA_LIB_NSM_PLUGIN_PATH), '--rename', cls.ALBA_LIB_NSM_PLUGIN_PATH])
            if not os.path.exists(cls.ALBA_LIB_PATH):
                os.mkdir(cls.ALBA_LIB_PATH)
            alba_old_binary_path_exists = os.path.exists(cls._to_old(cls.ALBA_BIN_PATH))
            if alba_old_binary_path_exists:
                check_output(['update-alternatives', '--install', cls.ALBA_BIN_PATH, 'alba', cls._to_old(cls.ALBA_BIN_PATH), cls.PRIORITY,
                              '--slave', cls.ALBA_LIB_ABM_PLUGIN_PATH, 'albamgr_plugin.cmxs', cls._to_old(cls.ALBA_LIB_ABM_PLUGIN_PATH),
                              '--slave', cls.ALBA_LIB_NSM_PLUGIN_PATH, 'nsm_host_plugin.cmxs', cls._to_old(cls.ALBA_LIB_NSM_PLUGIN_PATH)
                              ])
            check_output(['update-alternatives', '--install', cls.ALBA_BIN_PATH, 'alba', cls.ALBA_OPT_ALBA_BINARY_PATH.format(latest_alba_version[0]), cls.PRIORITY,
                          '--slave', cls.ALBA_LIB_ABM_PLUGIN_PATH, 'albamgr_plugin.cmxs', cls.ALBA_OPT_ABM_PLUGIN_PATH,
                          '--slave', cls.ALBA_LIB_NSM_PLUGIN_PATH, 'nsm_host_plugin.cmxs', cls.ALBA_OPT_NSM_PLUGIN_PATH])
        except CalledProcessError:
            raise InvalidAlbaVersionException('Invalid alba version has been found and used for updating alternatives: {0}'.format(latest_alba_version[0] or 'None'))


    @classmethod
    def update_binaries(cls):
        # type: () -> None
        """
        Update the binary
        :return:
        """
        if cls.BINARIES is None:
            raise NotImplementedError('Unable to update packages. Binaries are not included')
        all_package_names = chain.from_iterable([b[0] for b in cls.BINARIES])
        for package_name in all_package_names:
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
                return check_output(['arakoon', '--who-master', '-config', arakoon_config_url]).rstrip('/n')
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
        """
        Read an arakoon config from given url and return its value as a string
        :param config_url: str
        :return:
        """
        if config_url.startswith('arakoon://'):
            content = check_output(['alba', 'dev-extract-config', '--config', config_url])
        else:
            with open(config_url, 'r') as fh:
                content = fh.read()
        return content

    @classmethod
    def _to_old(cls, path):
        # type: (str) -> str
        """
        Automatically append the suffix .old to a path
        :param path: str
        :return:
        """
        return path+'.old'

    @classmethod
    def get_local_alba_versions(cls):
        # type: () -> Dict[str, LooseVersion]
        """
        Fetch the local alba versions, placed in the directory cls.OPT
        Return them in a dict eg. {'alba-1.5': LooseVersion(1.5)}
        :return:
        """
        alba_versions = {}
        try:
            for f_d in os.listdir(cls.OPT):
                match = cls.re_alba_binary_version.match(f_d)
                if match:
                    alba_versions[f_d] = LooseVersion(match.groupdict()['version'])
            return alba_versions
        except ValueError:
            raise RuntimeError('No valid alba binaries have been found in {0}. Not updating alternatives.'
                               'This might be caused by running this code before placing alba binaries in /opt.'.format(cls.OPT))

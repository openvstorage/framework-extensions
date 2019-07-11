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
Global Package Factory module inherited by all plugins
"""

import os
import copy
from subprocess import check_output
from .interfaces import PackageManagerBase, DebianPackage, RpmPackage


# noinspection PyUnreachableCode
if False:
    from typing import Set


class PackageFactory(object):
    """
    Factory class returning specialized classes
    """
    manager = None
    DISTRIBUTION_MAP = {'Ubuntu': DebianPackage,
                        'CentOS': RpmPackage}

    # Version commands
    VERSION_CMD_SD = "volumedriver_fs --version | grep version\: | grep -o [a-zA-Z0-9\.\-]*$"
    VERSION_CMD_ALBA = 'alba version --terse'
    VERSION_CMD_ARAKOON = "arakoon --version | grep version: | grep -o [a-zA-Z0-9\.\-]*$"

    # Update Components
    COMP_SD = 'storagedriver'
    COMP_FWK = 'framework'
    COMP_ALBA = 'alba'
    COMP_ISCSI = 'iscsi'
    COMP_ARAKOON = 'arakoon'  # This is the only component which cannot be selected via the GUI for update, but is used elsewhere

    # Migration Components
    COMP_MIGRATION_FWK = 'ovs'
    COMP_MIGRATION_ALBA = 'alba'
    COMP_MIGRATION_ISCSI = 'iscsi'

    # Editions
    EDITION_COMMUNITY = 'community'
    EDITION_ENTERPRISE = 'enterprise'

    # Packages
    PKG_ALBA = 'alba'
    PKG_ALBA_EE = 'alba-ee'
    PKG_ARAKOON = 'arakoon'

    PKG_OVS = 'openvstorage'
    PKG_OVS_ISCSI = 'openvstorage-iscsi-plugin'
    PKG_OVS_BACKEND = 'openvstorage-backend'
    PKG_OVS_EXTENSIONS = 'openvstorage-extensions'

    PKG_MGR_SDM = 'openvstorage-sdm'
    PKG_MGR_ISCSI = 'iscsi-manager'

    PKG_VOLDRV_BASE = 'volumedriver-no-dedup-base'
    PKG_VOLDRV_BASE_EE = 'volumedriver-ee-base'
    PKG_VOLDRV_SERVER = 'volumedriver-no-dedup-server'
    PKG_VOLDRV_SERVER_EE = 'volumedriver-ee-server'

    # Bundles
    # Community bundle of the base FWK without plugins
    BUNDLE_COMMUNITY = {'names': {COMP_FWK: {PKG_ARAKOON, PKG_OVS, PKG_OVS_EXTENSIONS},
                                  COMP_SD: {PKG_ARAKOON, PKG_VOLDRV_BASE, PKG_VOLDRV_SERVER},
                                  COMP_ALBA: {PKG_ALBA, PKG_ARAKOON}},
                        'binaries': {COMP_FWK: {PKG_ARAKOON},
                                     COMP_SD: {PKG_ARAKOON, PKG_VOLDRV_SERVER},
                                     COMP_ALBA: {PKG_ALBA, PKG_ARAKOON}},
                        'non_blocking': {PKG_OVS_EXTENSIONS},
                        'version_commands': {PKG_ALBA: VERSION_CMD_ALBA,
                                             PKG_ARAKOON: VERSION_CMD_ARAKOON,
                                             PKG_VOLDRV_BASE: VERSION_CMD_SD,
                                             PKG_VOLDRV_SERVER: VERSION_CMD_SD},
                        'mutually_exclusive': {PKG_VOLDRV_BASE_EE, PKG_VOLDRV_SERVER_EE, PKG_ALBA_EE}}

    BUNDLE_ENTERPRISE = {'names': {COMP_FWK: {PKG_ARAKOON, PKG_OVS, PKG_OVS_EXTENSIONS},
                                   COMP_SD: {PKG_ARAKOON, PKG_VOLDRV_BASE_EE, PKG_VOLDRV_SERVER_EE},
                                   COMP_ALBA: {PKG_ALBA_EE, PKG_ARAKOON}},
                         'binaries': {COMP_FWK: {PKG_ARAKOON},
                                      COMP_SD: {PKG_ARAKOON, PKG_VOLDRV_SERVER_EE},
                                      COMP_ALBA: {PKG_ALBA_EE, PKG_ARAKOON}},
                         'non_blocking': {PKG_OVS_EXTENSIONS},
                         'version_commands': {PKG_ALBA_EE: VERSION_CMD_ALBA,
                                              PKG_ARAKOON: VERSION_CMD_ARAKOON,
                                              PKG_VOLDRV_BASE_EE: VERSION_CMD_SD,
                                              PKG_VOLDRV_SERVER_EE: VERSION_CMD_SD},
                         'mutually_exclusive': {PKG_VOLDRV_BASE, PKG_VOLDRV_SERVER, PKG_ALBA}}

    BUNDLES = {EDITION_COMMUNITY: BUNDLE_COMMUNITY,
               EDITION_ENTERPRISE: BUNDLE_ENTERPRISE}

    @staticmethod
    def get_package_type():
        # type: () -> str
        """
        Determine the packager type
        :return: The package type
        :rtype: str
        """
        distributor = None
        check_lsb = check_output('which lsb_release 2>&1 || true', shell=True).strip()
        if "no lsb_release in" in check_lsb:
            if os.path.exists('/etc/centos-release'):
                distributor = 'CentOS'
        else:
            distributor = check_output('lsb_release -i', shell=True)
            distributor = distributor.replace('Distributor ID:', '').strip()
        return distributor

    @classmethod
    def get_manager(cls):
        # type: () -> PackageManagerBase
        """
        Returns a package manager
        """
        if cls.manager is None:
            distributor = cls.get_package_type()
            implementation_class = cls.DISTRIBUTION_MAP.get(distributor)
            if implementation_class:
                cls.manager = implementation_class(package_info=cls.get_package_info())
        if cls.manager is None:
            raise RuntimeError('Unknown PackageManager')

        return cls.manager

    @classmethod
    def get_package_info(cls):
        """
        Retrieve the package information related to the framework and it's plugins
        This must return a dictionary with keys: 'names', 'edition', 'binaries', 'non_blocking', 'version_commands' and 'mutually_exclusive'
            Names: These are the names of the packages split up per component related to this repository (framework)
                * Framework
                    * PKG_ARAKOON            --> Used for arakoon-config cluster and arakoon-ovsdb cluster
                    * PKG_OVS                --> base framework
                    * PKG_OVS_EXTENSIONS     --> Extensions code is used by the framework repository
                    * PKG_OVS_EXTENSIONS --> Extensions code is used by the framework-alba-plugin
                * StorageDriver
                    * PKG_ARAKOON            --> StorageDrivers make use of the arakoon-voldrv cluster
                    * PKG_VOLDRV_BASE(_EE)   --> Code for StorageDriver itself
                    * PKG_VOLDRV_SERVER(_EE) --> Code for StorageDriver itself
                    * PKG_ARAKOON        --> Used for arakoon-abm clusters and arakoon-nsm clusters. These also have a dependency to changes in the ALBA binary
                    * PKG_ALBA(_EE)      --> StorageDrivers deploy ALBA proxy services which depend on updates of the ALBA binary
            Edition: Used for different purposes
            Binaries: The names of the packages that come with a binary (also split up per component)
            Non Blocking: Packages which are potentially not yet available on all releases. These should be removed once every release contains these packages by default
            Version Commands: The commandos used to determine which binary version is currently active
            Mutually Exclusive: Packages which are not allowed to be installed depending on the edition. Eg: ALBA_EE cannot be installed on a 'community' edition
        :return: A dictionary containing information about the expected packages to be installed
        :rtype: dict
        """
        edition = cls.get_edition()
        bundle = copy.deepcopy(cls.BUNDLES[edition])
        bundle['edition'] = edition
        return edition

    @staticmethod
    def get_edition():
        # type: () -> str
        """
        Retrieve the edition through analysing the packages
        Fallback to community edition if nothing could be determined
        Note: Only checks by analysing the packages on the local node!
        :return: The edition
        :rtype: str
        """
        for version_cmd in [PackageFactory.VERSION_CMD_SD, PackageFactory.VERSION_CMD_ALBA]:
            try:
                return PackageFactory.EDITION_ENTERPRISE if 'ee-' in check_output([version_cmd], shell=True) else PackageFactory.EDITION_COMMUNITY
            except Exception:
                pass
        return PackageFactory.EDITION_COMMUNITY

    @classmethod
    def get_components(cls):
        # type: () -> Set[str]
        """
        Retrieve the components which relate to this repository
        :return: A set of components
        :rtype: set
        """
        return set()

    @classmethod
    def get_version_information(cls, client):
        """
        Validate whether the expected packages have been installed
        Validate whether no mutually exclusive packages have been installed. E.g.: alba cannot be installed in the EE edition
        Validate whether every installed package also has a candidate for installation
        :param client: SSHClient on which to retrieve the installed and candidate versions
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :raises ValueError:
            * When expected packages are not installed
            * When installed packages don't have an install candidate
            * When mutually exclusive packages have been installed
        :return: The versions of the running binaries, installed packages and candidate packages
        :rtype: tuple
        """
        pkg_mgr = cls.get_manager()
        pkg_info = cls.get_package_info()

        errors = set()
        all_installed = {}
        all_candidate = {}
        for component in cls.get_components():
            expected = pkg_info['names'].get(component, set())
            installed = pkg_mgr.get_installed_versions(client=client, package_names=expected)
            candidate = pkg_mgr.get_candidate_versions(client=client, package_names=expected)
            mutually_exclusive = pkg_mgr.get_installed_versions(client=client, package_names=pkg_info['mutually_exclusive'])

            for package_name in mutually_exclusive:
                errors.add('Package {0}: This package should not have been installed'.format(package_name))
            for package_name in expected - set(installed) - pkg_info['non_blocking']:
                errors.add('Package {0}: Not installed while it should be'.format(package_name))
            for package_name in set(installed) - set(candidate) - pkg_info['non_blocking']:
                errors.add('Package {0}: Missing candidate for installation'.format(package_name))

            all_installed[component] = installed
            all_candidate[component] = candidate

        if len(errors) > 0:
            raise ValueError('Errors found in the packages:\n * {0}'.format('\n * '.join(errors)))
        return all_installed, all_candidate

    @classmethod
    def get_packages_to_update(cls, client):
        """
        Compare the versions of the installed and candidate packages and return the packages which have a different version.
        :return: The packages that need to be updated
        :rtype: dict
        """
        installed, candidate = cls.get_version_information(client=client)
        update_info = {}
        for component in cls.get_components():
            for package_name in installed[component]:
                installed_version = installed[component][package_name]
                candidate_version = candidate[component][package_name]
                if installed_version < candidate_version:
                    if component not in update_info:
                        update_info[component] = {}
                    update_info[component][package_name] = {'installed': str(installed_version),
                                                            'candidate': str(candidate_version)}
        return update_info

    @classmethod
    def get_package_and_version_cmd_for(cls, component):
        """
        Retrieve the installed package related to the component specified. E.g. will return 'alba-ee' for component alba when the EE edition has been installed
        Retrieve the command to retrieve the running version of the binary related to the specified component. E.g. will return 'alba version --terse' for alba component
        :param component: Component to retrieve the information for
        :type component: str
        :raises ValueError:
            * When unknown component is specified
            * When related package for the specified component could not be retrieved
        :return: The package installed and the command to retrieve the version of the binary
        :rtype: tuple
        """
        components = [cls.COMP_ALBA, cls.COMP_ARAKOON, cls.COMP_SD]
        if component not in components:
            raise ValueError('Component {0} is not supported'.format(component))

        edition = cls.get_package_info()['edition']
        if component == cls.COMP_ALBA:
            version_cmd = cls.VERSION_CMD_ALBA
            package_name = cls.PKG_ALBA_EE if edition == 'enterprise' else cls.PKG_ALBA
        elif component == cls.COMP_SD:
            version_cmd = cls.VERSION_CMD_SD
            package_name = cls.PKG_VOLDRV_SERVER_EE if edition == 'enterprise' else cls.PKG_VOLDRV_SERVER
        else:
            version_cmd = cls.VERSION_CMD_ARAKOON
            package_name = cls.PKG_ARAKOON
        return package_name, version_cmd

    @classmethod
    def update_packages(cls, client, packages, logger):
        """
        Update the requested packages
        :param client: The SSHClient on which to update the packages
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param packages: The packages to update. Structure {<pkg_name>: {'installed': <version1>, 'candidate': <version2>}}
        :type packages: dict
        :param logger: Logger instance
        :type logger: ovs_extensions.log.logger.Logger
        :return: A boolean whether to abort the entire update process or not
        :rtype: bool
        """
        # Always install the extensions package first
        package_names = sorted(packages)
        if PackageFactory.PKG_OVS_EXTENSIONS in package_names:
            package_names.remove(PackageFactory.PKG_OVS_EXTENSIONS)
            package_names.insert(0, PackageFactory.PKG_OVS_EXTENSIONS)

        abort = False
        package_mgr = cls.get_manager()
        for package_name in package_names:
            try:
                installed = packages[package_name]['installed']
                candidate = packages[package_name]['candidate']
                installed_versions = package_mgr.get_installed_versions(client=client, package_names=[package_name])
                if candidate == str(installed_versions.get(package_name)):
                    # Package has already been installed by another process
                    continue

                logger.debug('{0}: Updating package {1} ({2} --> {3})'.format(client.ip, package_name, installed, candidate))
                package_mgr.install(package_name=package_name, client=client)
                logger.debug('{0}: Updated package {1}'.format(client.ip, package_name))
            except Exception:
                logger.exception('{0}: Updating package {1} failed'.format(client.ip, package_name))
                abort = True
        return abort

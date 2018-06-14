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
NBD controller module
"""
import os
import apt
import yaml
from ovs_extensions.constants.framework import NBD_ID
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.system import System
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class NBDManager(object):
    """
    Abstract implementation of the NBD Manager object to create, start, stop or remove NBD device services on this node.
    """

    DEVICE_PATH = '/dev/{0}'
    SERVICE_NAME = 'ovs-{0}-{1}'
    NBD_SERVICE_NAME = '{0}_{1}'
    MINIMAL_BLOCK_SIZE = 32 * 1024
    OPT_CONFIG_PATH = '/etc/ovs_nbd/{0}'
    NODE_PATH = NBD_ID
    SERVICE_FILE_PATH = '/usr/lib/python2.7/dist-packages/ovs_extensions/config/{0}/'

    # Service file settings: to be overruled
    SERVICE_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nbdservice.py')  # Working directory for the service
    WORKING_DIRECTORY = '/usr/lib/python2.7/dist-packages/ovs_extensions/'  # Working directory for the service
    MODULE_PATH = ''  # Empty for the extensions as the ovs_extensions package is found under dist-packages
    MANAGER_SERVICE_NAME = ''

    def __init__(self):
        # type: () -> None
        self._configuration = self._get_configuration()
        self._service_manager = self._get_service_manager()
        self._client = SSHClient('127.0.0.1', username='root')

    @staticmethod
    def _get_configuration():
        raise NotImplementedError()

    @staticmethod
    def _get_service_manager():
        raise NotImplementedError()

    def create_service(self, volume_uri, block_size=MINIMAL_BLOCK_SIZE):
        # type: (str, int) -> str
        """
        Create NBD service
        :param volume_uri: tcp://user:pass@ip:port/volume-name
        :param block_size: block size in bytes
        :return: path /dev/nbdx
        :raises: RuntimeError if volume uri -ip:port does not match ip regex
                                            -tcp does not match tcp connection regex
                                 block size is too small or no integer
                                 volumedriver-nbd package is not installed
        """

        # Unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            node_id = 'unittest_guid'
        else:
            node_id = System.get_my_machine_id().strip()

        # Parameter verification
        cache = apt.Cache()
        try:
            cache['volumedriver-nbd'].is_installed
        except KeyError:
            raise RuntimeError('Package volumedriver-nbd is not yet installed')
        if type(volume_uri) != str:
            raise RuntimeError('Invalid parameter: {0} should be of type `str`'.format(volume_uri))
        if type(block_size) != int or block_size < self.MINIMAL_BLOCK_SIZE:
            raise RuntimeError('Invalid parameter: {0} should be of type `int` and bigger then > {1}'.format(block_size, self.MINIMAL_BLOCK_SIZE))

        node_path = self.NODE_PATH.format(node_id)
        user_pass, ip_port = volume_uri.split('@')
        ip_port, vol_name = ip_port.split('/')

        ExtensionsToolbox.verify_required_params(required_params={'user_pass': (str, ExtensionsToolbox.regex_tcp_conn, True),
                                                                  'ip_port': (str, ExtensionsToolbox.regex_ip_port, True),
                                                                  'vol_name': (str, None, True)},
                                                 actual_params={'user_pass': user_pass, 'ip_port': ip_port, 'vol_name': vol_name},
                                                 verify_keys=True)

        nbd_number = self._find_first_free_device_number(node_path)
        config_path = os.path.join(node_path, nbd_number, 'config')

        # Set self._configuration keys and values in local config
        nbd_path = self.DEVICE_PATH.format(nbd_number)
        config_settings = {'volume_uri': volume_uri,
                           'nbd_path': nbd_path}
        if block_size > NBDManager.MINIMAL_BLOCK_SIZE:
            config_settings['libovsvoldrv_request_split_size'] = block_size
        self._configuration.set(key=config_path, value=yaml.dump(config_settings, default_flow_style=False), raw=True)

        # Add service
        opt_config_path = self.OPT_CONFIG_PATH.format(nbd_number)
        if not self._client.file_exists(opt_config_path):
            self._client.file_create(opt_config_path)
        self._service_manager.add_service(name='nbd',
                                          client=self._client,
                                          params={'NODE_ID': str(node_id),
                                                  'NBDX': nbd_number,
                                                  'SCRIPT': self.SERVICE_SCRIPT_PATH,
                                                  'WD': self.WORKING_DIRECTORY,  # Module path and wd depend on the module the nbd service is called in eg. ISCSI manager
                                                  'MODULE_PATH': self.MODULE_PATH,
                                                  'MGR_SERVICE': self.MANAGER_SERVICE_NAME},
                                          target_name=self.SERVICE_NAME.format(nbd_number, vol_name),
                                          path=self.SERVICE_FILE_PATH)
        return nbd_path

    def _find_first_free_device_number(self, node_path):
        # type: (str) -> str
        """
        Find first device number that is not in use
        :param node_path: path on the node where devices can be found
        :return: nbdX
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            nbd_number = 'nbd_unittest_number'
        else:
            if self._configuration.dir_exists(node_path):
                nbd_numbers = [int(i.lstrip('nbd')) for i in list(self._configuration.list(node_path))]
                found = False
                starting_number = 0
                while found is False:
                    if starting_number in nbd_numbers:
                        starting_number += 1
                    else:
                        found = True
            else:
                starting_number = 0
            nbd_number = 'nbd{0}'.format(starting_number).strip()
        return nbd_number

    def _get_service_file_path(self, nbd_path):
        # type: (str) -> str
        """
        Get the arakoon service file path of the given service
        :param nbd_path: /dev/nbdx
        :return: return the service config path
        :raises: RuntimeError when multiple or no paths are found
        """
        nbd_number = nbd_path.split('/')[-1]
        local_id = System.get_my_machine_id()
        paths = [i for i in self._configuration.list(self.NODE_PATH.format(local_id), recursive=True) if i.endswith('config') and nbd_number in i]

        if len(paths) > 1:
            raise RuntimeError('More then 1 path has been found for given nbd_path: {0}'.format(paths))
        if len(paths) == 0:
            raise RuntimeError('No configpath has been found for given device')
        return paths[0]

    def _get_vol_name(self, nbd_path):
        # type: (str) -> str
        """
        Parse volume name from config file specified for nbd_path
        :param nbd_path: /dev/nbdx
        :return: volume name
        """
        nbd_service_path = self._get_service_file_path(nbd_path)
        content = self._configuration.get(nbd_service_path, raw=True)
        content = yaml.load(content)
        vol_name = content.get('volume_uri').rsplit('/')[-1]
        return vol_name

    def destroy_device(self, nbd_path):
        # type: (str) -> None
        """
        Destroy NBD device with given path
        :param nbd_path: /dev/NBDX
        :return: whether or not the destroy action failed
        :raises OSError
        """
        nbd_number = nbd_path.split('/')[-1]
        vol_name = self._get_vol_name(nbd_path)
        if self._service_manager.has_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client):
            self._service_manager.stop_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client)
            self._service_manager.remove_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client)
        path_to_delete = str(os.path.join(self.NODE_PATH.format(System.get_my_machine_id().strip()), nbd_number))  # Casting to string as the DAL might have returned a unicode
        self._configuration.delete(path_to_delete)
        try:
            os.remove(self.OPT_CONFIG_PATH.format(nbd_number))
        except OSError:
            pass

    def start_device(self, nbd_path):
        # type: (str) -> None
        """
        Start NBD device with given path
        :param nbd_path: /dev/NBDX
        :return: whether or not the start action succeeded
        """
        nbd_number = nbd_path.rsplit('/')[-1]
        vol_name = self._get_vol_name(nbd_path)
        if self._service_manager.has_service(self.SERVICE_NAME.format(nbd_number, vol_name), self._client):
            self._service_manager.start_service(self.SERVICE_NAME.format(nbd_number, vol_name), self._client)

    def stop_device(self, nbd_path):
        # type: (str) -> None
        """
        Stop the NBD device with the given /dev/nbdx path on current node
        :param nbd_path: /dev/NBDX
        :return: None
        """
        nbd_number = nbd_path.split('/')[-1]
        vol_name = self._get_vol_name(nbd_path)
        if self._service_manager.has_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client):
            self._service_manager.stop_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client)

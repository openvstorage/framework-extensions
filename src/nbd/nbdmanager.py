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
import sys
import yaml
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from ovs_extensions.generic.sshclient import SSHClient
from ovs_extensions.generic.system import System


class NBDManager(object):

    """
    NBD Controller object
    """

    DEVICE_PATH = '/dev/{0}'
    SERVICE_NAME = 'ovs-{0}-{1}'
    NBD_SERVICE_NAME = '{0}_{1}'
    MINIMAL_BLOCK_SIZE = 32 * 1024
    OPT_CONFIG_PATH = '/etc/ovs_nbd/{0}'
    BASE_PATH = '/ovs/framework/nbdnodes'
    NODE_PATH = os.path.join(BASE_PATH, '{0}')
    SERVICE_FILE_PATH = '/usr/lib/python2.7/dist-packages/ovs_extensions/config/{0}/'

    # Service file settings: to be overruled
    SERVICE_SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nbdservice.py')  # Working directory for the service
    WORKING_DIRECTORY = '/usr/lib/python2.7/dist-packages/ovs_extensions/'  # Working directory for the service
    MODULE_PATH = ''  # Empty for the extensions as the ovs_extensions package is found under dist-packages

    def __init__(self):
        self._configuration = self._get_configuration()
        self._service_manager = self._get_service_manager()
        self._client = SSHClient('127.0.0.1', username='root')

    @staticmethod
    def _get_configuration():
        raise NotImplementedError()

    @staticmethod
    def _get_service_manager():
        raise NotImplementedError()

    def create_service(self, volume_uri, block_size=MINIMAL_BLOCK_SIZE, node_id=None, number=None):
        """
        Create NBD service
        :param volume_uri: tcp://user:pass@ip:port/volume-name
        :param block_size: block size in bytes
        :return: (boolean, path)
        """

        # unittests
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            node_id = 'unittest_guid'
        elif node_id is not None:
            node_id = node_id
        else:
            node_id = System.get_my_machine_id()
        node_id = node_id.strip()
        # Parameter verification
        if type(volume_uri) != str:
            raise RuntimeError
        if type(block_size) != int or block_size < self.MINIMAL_BLOCK_SIZE:
            raise RuntimeError
        user_pass, ip_port = volume_uri.split('@')
        ip_port, vol_name = ip_port.split('/')
        ExtensionsToolbox.verify_required_params(required_params={'user_pass': (str, ExtensionsToolbox.regex_tcp_conn, True),
                                                                  'ip_port': (str, ExtensionsToolbox.regex_ip_port, True),
                                                                  'vol_name': (str, None, True)},
                                                 actual_params={'user_pass': user_pass, 'ip_port': ip_port, 'vol_name': vol_name},
                                                 verify_keys=True)
        node_path = self.NODE_PATH.format(node_id)

        # Find first device number that is not in use
        if number is None:
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
        else:
            starting_number = number
        nbd_number = 'nbd{0}'.format(starting_number).strip()
        config_path = node_path+'/'+nbd_number+'/config'
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
                                                  'WD': self.WORKING_DIRECTORY,
                                                  'MODULE_PATH': self.MODULE_PATH},

                                          target_name=self.SERVICE_NAME.format(nbd_number, vol_name),
                                          path=self.SERVICE_FILE_PATH)
        return True, nbd_path

    def _get_service_path(self, nbd_path):
        nbd_number = nbd_path.split('/')[-1]
        paths = [i for i in self._configuration.list(self.BASE_PATH, recursive=True) if i.endswith('config') and nbd_number in i]

        if len(paths) > 1:
            raise RuntimeError('More then 1 path has been found for given nbd_path: {0}'.format(paths))
        if len(paths) == 0:
            raise RuntimeError('No configpath has been found for given device')
        return paths[0]

    def _get_vol_name(self, nbd_path):
        nbd_service_path = self._get_service_path(nbd_path)
        content = self._configuration.get(nbd_service_path, raw=True)
        content = content.split('\n')
        content = [i.split(':', 1) for i in content if i != ['']]
        d = {}
        for i in content:
            if i != ['']:
                d[i[0]] = i[1]
        vol_name = d.get('volume_uri').split('/')[-1]
        return vol_name

    def destroy_device(self, nbd_path):
        """
        Destroy NBD device with given path
        :param nbd_path: /dev/NBDX
        :return: whether or not the destroy action failed
        """
        nbd_number = nbd_path.split('/')[-1]
        vol_name = self._get_vol_name(nbd_path)
        if self._service_manager.has_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client):
            self._service_manager.stop_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client, timeout=60)
            self._service_manager.remove_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client)
        self._configuration.delete(self._get_service_path(nbd_path))
        try:
            os.remove(self.OPT_CONFIG_PATH.format(nbd_number))
        except OSError:
            pass

    def start_device(self, nbd_path):
        """
        Start NBD device with given path
        :param nbd_path: /dev/NBDX
        :return: whether or not the start action succeeded
        """
        nbd_number = nbd_path.split('/')[-1]
        vol_name = self._get_vol_name(nbd_path)
        if self._service_manager.has_service(self.SERVICE_NAME.format(nbd_number, vol_name), self._client):
            self._service_manager.start_service(self.SERVICE_NAME.format(nbd_number, vol_name), self._client)


    def stop_device(self, nbd_path):
        """
        Stop the NBD device with the given /dev/nbdx path on current node
        :param nbd_path: /dev/NBDX
        :return: whether or not the stop device action succeeded
        """
        nbd_number = nbd_path.split('/')[-1]
        vol_name = self._get_vol_name(nbd_path)
        if self._service_manager.has_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client):
            self._service_manager.stop_service(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client)

        if self._service_manager.get_service_status(self.SERVICE_NAME.format(nbd_number, vol_name), client=self._client) == 'inactive':
            return True
        else:
            return False


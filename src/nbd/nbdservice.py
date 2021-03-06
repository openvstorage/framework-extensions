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
import os
import yaml
import argparse
from ovs_extensions.nbd.nbdmanager import NBDManager


class NBDService(object):
    """
    Abstract implementation of the NBD Service object to create, start, stop or remove NBD device services on this node.
    """

    @classmethod
    def _get_configuration(cls):
        """
        To be overruled by the module instance of this abstract class
        """
        raise NotImplementedError('No Configuration has been implemented')

    @classmethod
    def regenerate_config(cls, nbdx, node_id):
        """
        Removes the old config file of the service, downloads latest config from the config management and puts it in place of the old config file.
        :param nbdx: nbd device
        :param node_id: guid of the node this service is
        :return: None
        """
        configuration = cls._get_configuration()
        nbd_config_file_path = NBDManager.OPT_CONFIG_PATH.format(nbdx)
        try:
            os.remove(nbd_config_file_path)
        except OSError:
            pass
        content = configuration.get('/ovs/framework/nbdnodes/{0}/{1}/config.raw'.format(node_id, nbdx))
        content_dict = yaml.load(content)
        with open(nbd_config_file_path, 'w') as nbd_config_file:
            nbd_config_file.write(yaml.dump(content_dict, default_flow_style=False))

    @classmethod
    def run(cls):
        """
        Run the service. This method is meant to be invoked from if __name__ == '__main__' of the implementation files
        :return: None
        """
        parser = argparse.ArgumentParser(prog='nbd-refresh-config', description='Refresh the config file of the service')
        parser.add_argument('NODE_ID')
        parser.add_argument('NBDX')
        arguments = parser.parse_args()
        cls.regenerate_config(node_id=arguments.NODE_ID, nbdx=arguments.NBDX)


if __name__ == '__main__':
    NBDService.run()

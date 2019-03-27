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
ArakoonNodeConfig class
ArakoonClusterConfig class
ArakoonInstaller class
"""

import os
import json
from ConfigParser import RawConfigParser
from StringIO import StringIO
from ovs_extensions.generic.sshclient import CalledProcessError, SSHClient
from ovs_extensions.log.logger import Logger

ARAKOON_CLUSTER_TYPES = ['ABM', 'FWK', 'NSM', 'SD', 'CFG']


class ArakoonNodeConfig(object):
    """
    cluster node config parameters
    """
    def __init__(self, name, ip, client_port, messaging_port, log_sinks, crash_log_sinks, home, tlog_dir, preferred_master=False, fsync=True, log_level='info', tlog_compression='snappy'):
        """
        Initializes a new Config entry for a single Node
        """
        self.ip = ip
        self.home = home
        self.name = name
        self.fsync = fsync
        self.tlog_dir = tlog_dir
        self.log_level = log_level
        self.log_sinks = log_sinks
        self.client_port = client_port
        self.messaging_port = messaging_port
        self.crash_log_sinks = crash_log_sinks
        self.tlog_compression = tlog_compression
        self.preferred_master = preferred_master

    def __hash__(self):
        """
        Defines a hashing equivalent for a given ArakoonNodeConfig
        """
        return hash(self.name)

    def __eq__(self, other):
        """
        Checks whether two objects are the same.
        """
        if not isinstance(other, ArakoonNodeConfig):
            return False
        return self.__hash__() == other.__hash__()

    def __ne__(self, other):
        """
        Checks whether two objects are not the same.
        """
        if not isinstance(other, ArakoonNodeConfig):
            return True
        return not self.__eq__(other)


class ArakoonClusterConfig(object):
    """
    contains cluster config parameters
    """
    CONFIG_ROOT = '/ovs/arakoon'
    CONFIG_KEY = CONFIG_ROOT + '/{0}/config'
    CONFIG_FILE = '/opt/OpenvStorage/config/arakoon_{0}.ini'

    def __init__(self, cluster_id, load_config=True, source_ip=None, plugins=None, configuration=None):
        """
        Initializes an empty Cluster Config
        """
        self.plugins = []
        self._extra_globals = {'tlog_max_entries': 5000}
        if isinstance(plugins, list):
            self.plugins = plugins
        elif isinstance(plugins, basestring):
            self.plugins.append(plugins)

        if configuration is not None:
            self._configuration = configuration
        else:
            self._configuration = self._get_configuration()
        self.nodes = []
        self.source_ip = source_ip
        self.cluster_id = cluster_id
        if self.source_ip is None:
            self.internal_config_path = ArakoonClusterConfig.CONFIG_KEY.format(cluster_id)
            self.external_config_path = self._configuration.get_configuration_path(self.internal_config_path)
        else:
            self.internal_config_path = ArakoonClusterConfig.CONFIG_FILE.format(cluster_id)
            self.external_config_path = self.internal_config_path

        if load_config is True:
            self.read_config(ip=self.source_ip)

    def load_client(self, ip):
        """
        Create an SSHClient instance to the IP provided
        :param ip: IP for the SSHClient
        :type ip: str
        :return: An SSHClient instance
        :rtype: ovs_extensions.generic.sshclient.SSHClient
        """
        if self.source_ip is not None:
            if ip is None:
                raise RuntimeError('An IP should be passed for filesystem configuration')
            return SSHClient(ip, username=ArakoonInstaller.SSHCLIENT_USER)

    def read_config(self, ip=None, contents=None):
        """
        Constructs a configuration object from config contents
        :param ip: IP on which the configuration file resides (Only for filesystem Arakoon clusters)
        :type ip: str
        :param contents: Contents to parse
        :type contents: str
        :return: None
        :rtype: NoneType
        """
        if contents is None:
            if ip is None:
                contents = self._configuration.get(self.internal_config_path, raw=True)
            else:
                client = self.load_client(ip)
                contents = client.file_read(self.internal_config_path)

        parser = RawConfigParser()
        try:
            parser.readfp(StringIO(contents))
            self.nodes = []
            self._extra_globals = {}
            preferred_masters = []
            for key in parser.options('global'):
                if key == 'plugins':
                    self.plugins = [plugin.strip() for plugin in parser.get('global', 'plugins').split(',')]
                elif key == 'cluster_id':
                    self.cluster_id = parser.get('global', 'cluster_id')
                elif key == 'cluster':
                    pass  # Ignore these
                elif key == 'preferred_masters':
                    preferred_masters = parser.get('global', key).split(',')
                else:
                    self._extra_globals[key] = parser.get('global', key)
            for node in parser.get('global', 'cluster').split(','):
                node = node.strip()
                self.nodes.append(ArakoonNodeConfig(ip=parser.get(node, 'ip'),
                                                    name=node,
                                                    home=parser.get(node, 'home'),
                                                    fsync=parser.getboolean(node, 'fsync'),
                                                    tlog_dir=parser.get(node, 'tlog_dir'),
                                                    log_sinks=parser.get(node, 'log_sinks'),
                                                    log_level=parser.get(node, 'log_level'),
                                                    client_port=parser.getint(node, 'client_port'),
                                                    messaging_port=parser.getint(node, 'messaging_port'),
                                                    crash_log_sinks=parser.get(node, 'crash_log_sinks'),
                                                    tlog_compression=parser.get(node, 'tlog_compression'),
                                                    preferred_master=node in preferred_masters))
        except Exception as ex:
            raise NoSectionError('{0} on {1}'.format(ex, self.cluster_id))

    def write_config(self, ip=None):
        """
        Writes the configuration down to in the format expected by Arakoon
        """
        contents = self.export_ini()
        if self.source_ip is None:
            self._configuration.set(self.internal_config_path, contents, raw=True)
        else:
            client = self.load_client(ip)
            client.file_write(self.internal_config_path, contents)

    def delete_config(self, ip=None):
        """
        Deletes a configuration file
        :return: None
        :rtype: NoneType
        """
        if self.source_ip is None:
            key = self.internal_config_path
            if self._configuration.exists(key, raw=True):
                self._configuration.delete(key, raw=True)
        else:
            client = self.load_client(ip)
            client.file_delete(self.internal_config_path)

    def export_dict(self):
        """
        Exports the current configuration to a python dict
        :return: Data available in the Arakoon configuration
        :rtype: dict
        """
        data = {'global': {'cluster_id': self.cluster_id,
                           'cluster': ','.join(sorted(node.name for node in self.nodes))}}
        if len(self.plugins) > 0:
            data['global']['plugins'] = ','.join(sorted(self.plugins))
        preferred_masters = [node.name for node in self.nodes if node.preferred_master is True]
        if len(preferred_masters) > 0:
            data['global']['preferred_masters'] = ','.join(preferred_masters)
        for key, value in self._extra_globals.iteritems():
            data['global'][key] = value
        for node in self.nodes:
            data[node.name] = {'ip': node.ip,
                               'home': node.home,
                               'name': node.name,
                               'fsync': 'true' if node.fsync else 'false',
                               'tlog_dir': node.tlog_dir,
                               'log_level': node.log_level,
                               'log_sinks': node.log_sinks,
                               'client_port': node.client_port,
                               'messaging_port': node.messaging_port,
                               'crash_log_sinks': node.crash_log_sinks,
                               'tlog_compression': node.tlog_compression}
        return data

    def export_ini(self):
        """
        Exports the current configuration to an ini file format
        :return: Arakoon configuration in string format
        :rtype: str
        """
        contents = RawConfigParser()
        data = self.export_dict()
        sections = data.keys()
        sections.remove('global')
        for section in ['global'] + sorted(sections):
            contents.add_section(section)
            for item in sorted(data[section]):
                contents.set(section, item, data[section][item])
        config_io = StringIO()
        contents.write(config_io)
        return str(config_io.getvalue())

    def import_config(self, config):
        """
        Imports a configuration into the ArakoonClusterConfig instance
        :return: None
        :rtype: NoneType
        """
        config = self.convert_config_to(config=config, return_type='DICT')
        new_sections = sorted(config.keys())
        old_sections = sorted([node.name for node in self.nodes] + ['global'])
        if old_sections != new_sections:
            raise ValueError('To add/remove sections, please use extend_cluster/shrink_cluster')

        for section, info in config.iteritems():
            if section == 'global':
                continue
            if info['name'] != section:
                raise ValueError('Names cannot be updated')

        self.nodes = []
        self._extra_globals = {}
        preferred_masters = []
        for key, value in config['global'].iteritems():
            if key == 'plugins':
                self.plugins = [plugin.strip() for plugin in value.split(',')]
            elif key == 'cluster_id':
                self.cluster_id = value
            elif key == 'cluster':
                pass
            elif key == 'preferred_masters':
                preferred_masters = value.split(',')
            else:
                self._extra_globals[key] = value
        del config['global']
        for node_name, node_info in config.iteritems():
            self.nodes.append(ArakoonNodeConfig(ip=node_info['ip'],
                                                name=node_name,
                                                home=node_info['home'],
                                                fsync=node_info['fsync'] == 'true',
                                                tlog_dir=node_info['tlog_dir'],
                                                log_level=node_info['log_level'],
                                                log_sinks=node_info['log_sinks'],
                                                client_port=int(node_info['client_port']),
                                                messaging_port=int(node_info['messaging_port']),
                                                crash_log_sinks=node_info['crash_log_sinks'],
                                                tlog_compression=node_info['tlog_compression'],
                                                preferred_master=node_name in preferred_masters))

    @classmethod
    def get_cluster_name(cls, internal_name):
        """
        Retrieve the name of the cluster
        :param internal_name: Name as known by the framework
        :type internal_name: str
        :return: Name known by user
        :rtype: str
        """
        config_key = '/ovs/framework/arakoon_clusters'
        configuration = cls._get_configuration()
        if configuration.exists(config_key):
            cluster_info = configuration.get(config_key)
            if internal_name in cluster_info:
                return cluster_info[internal_name]
        if internal_name not in ['ovsdb', 'voldrv']:
            return internal_name

    @classmethod
    def convert_config_to(cls, config, return_type):
        """
        Convert an Arakoon Cluster Config to another format (DICT or INI)
        :param config: Arakoon Cluster Config representation
        :type config: dict|str
        :param return_type: Type in which the config needs to be returned (DICT or INI)
        :type return_type: str
        :return: If config is DICT, INI format is returned and vice versa
        """
        if return_type not in ['DICT', 'INI']:
            raise ValueError('Unsupported return_type specified')
        if not isinstance(config, dict) and not isinstance(config, basestring):
            raise ValueError('Config should be a dict or basestring representation of an Arakoon cluster config')

        if (isinstance(config, dict) and return_type == 'DICT') or (isinstance(config, basestring) and return_type == 'INI'):
            return config

        # DICT --> INI
        if isinstance(config, dict):
            rcp = RawConfigParser()
            for section in config:
                rcp.add_section(section)
                for key, value in config[section].iteritems():
                    rcp.set(section, key, value)
            config_io = StringIO()
            rcp.write(config_io)
            return str(config_io.getvalue())

        # INI --> DICT
        if isinstance(config, basestring):
            converted = {}
            rcp = RawConfigParser()
            rcp.readfp(StringIO(config))
            for section in rcp.sections():
                converted[section] = {}
                for option in rcp.options(section):
                    if option in ['client_port', 'messaging_port']:
                        converted[section][option] = rcp.getint(section, option)
                    else:
                        converted[section][option] = rcp.get(section, option)
            return converted

    @classmethod
    def _get_configuration(cls):
        raise NotImplementedError()


class ArakoonInstaller(object):
    """
    Class to dynamically install/(re)configure Arakoon cluster
    """
    ARAKOON_HOME_DIR = '{0}/arakoon/{1}/db'
    ARAKOON_TLOG_DIR = '{0}/arakoon/{1}/tlogs'
    SSHCLIENT_USER = 'ovs'
    METADATA_KEY = '__ovs_metadata'
    INTERNAL_CONFIG_KEY = '__ovs_config'

    def __init__(self, cluster_name):
        """
        ArakoonInstaller constructor
        :param cluster_name: Name of the cluster
        :type cluster_name: str
        """
        self.config = None
        self.metadata = None
        self.cluster_name = cluster_name
        self.service_metadata = {}

        self._system = self._get_system()
        self._logger = self._get_logger_instance()
        self._configuration = self._get_configuration()
        self._service_manager = self._get_service_manager()

    def load(self, ip=None):
        self.config = ArakoonClusterConfig(cluster_id=self.cluster_name,
                                           source_ip=ip,
                                           configuration=self._configuration)

    @property
    def is_filesystem(self):
        return self.config.source_ip is not None

    @property
    def ports(self):
        return dict((node.ip, [node.client_port, node.messaging_port]) for node in self.config.nodes)

    def create_cluster(self, cluster_type, ip, base_dir, log_sinks, crash_log_sinks, plugins=None, locked=True, internal=True, port_range=None, preferred_master=False):
        """
        Always creates a cluster but marks it's usage according to the internal flag
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :type cluster_type: str
        :param ip: IP address of the first node of the new cluster
        :type ip: str
        :param base_dir: Base directory that should contain the data and tlogs
        :type base_dir: str
        :param log_sinks: Logsink
        :type log_sinks: str
        :param crash_log_sinks: Logsink for crash reports
        :type crash_log_sinks: str
        :param plugins: Plugins that should be added to the configuration file
        :type plugins: dict
        :param locked: Indicates whether the create should run in a locked context (e.g. to prevent port conflicts)
        :type locked: bool
        :param internal: Is cluster internally managed by OVS
        :type internal: bool
        :param port_range: Range of ports which should be used for the Arakoon processes (2 available ports in the range will be selected) eg: [26400, 26499]
        :type port_range: list
        :param preferred_master: Indicate this node as 1 of the preferred masters during master election
        :type preferred_master: bool
        :return: Ports used by the cluster, metadata of the cluster and metadata of the service
        :rtype: dict
        """
        if cluster_type not in ARAKOON_CLUSTER_TYPES:
            raise ValueError('Cluster type {0} is not supported. Please choose from {1}'.format(cluster_type, ', '.join(sorted(ARAKOON_CLUSTER_TYPES))))
        if plugins is not None and not isinstance(plugins, dict):
            raise ValueError('Plugins should be a dict')

        client = SSHClient(endpoint=ip, username=ArakoonInstaller.SSHCLIENT_USER)
        filesystem = cluster_type == 'CFG'
        if filesystem is True:
            exists = client.file_exists(ArakoonClusterConfig.CONFIG_FILE.format(self.cluster_name))
        else:
            exists = self._configuration.dir_exists('/ovs/arakoon/{0}'.format(self.cluster_name))
        if exists is True:
            raise ValueError('An Arakoon cluster with name "{0}" already exists'.format(self.cluster_name))

        self._logger.debug('Creating cluster {0} of type {1} on {2}'.format(self.cluster_name, cluster_type, ip))

        node_name = self._system.get_my_machine_id(client)
        base_dir = base_dir.rstrip('/')
        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, self.cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, self.cluster_name)
        self.clean_leftover_arakoon_data(ip, [home_dir, tlog_dir])

        port_mutex = None
        try:
            if locked is True:
                volatile_mutex = self._get_volatile_mutex()
                port_mutex = volatile_mutex('arakoon_install_ports_{0}'.format(ip))
                port_mutex.acquire(wait=60)

            if filesystem is True:
                if port_range is None:
                    port_range = [26400]
                ports = self._system.get_free_ports(selected_range=port_range, nr=2, client=client)
            else:
                ports = self._get_free_ports(client=client, port_range=port_range)

            self.config = ArakoonClusterConfig(cluster_id=self.cluster_name,
                                               source_ip=ip if filesystem is True else None,
                                               load_config=False,
                                               configuration=self._configuration)
            self.config.plugins = plugins.keys() if plugins is not None else []
            self.config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                       ip=ip,
                                                       client_port=ports[0],
                                                       messaging_port=ports[1],
                                                       log_sinks=log_sinks,
                                                       crash_log_sinks=crash_log_sinks,
                                                       home=home_dir,
                                                       tlog_dir=tlog_dir,
                                                       preferred_master=preferred_master))
            self.metadata = {'internal': internal,
                             'cluster_name': self.cluster_name,
                             'cluster_type': cluster_type,
                             'in_use': False}
            self._deploy(plugins=plugins.values() if plugins is not None else None,
                         delay_service_registration=filesystem)
        finally:
            if port_mutex is not None:
                port_mutex.release()

        self._logger.debug('Creating cluster {0} of type {1} on {2} completed'.format(self.cluster_name, cluster_type, ip))

    def delete_cluster(self):
        """
        Deletes a complete cluster
        :return: None
        :rtype: NoneType
        """
        self._logger.debug('Deleting cluster {0}'.format(self.cluster_name))
        if self.config is None:
            raise RuntimeError('Config not yet loaded')
        service_name = self.get_service_name_for_cluster(cluster_name=self.cluster_name)
        for node in self.config.nodes:
            try:
                self._service_manager.unregister_service(service_name=service_name, node_name=node.name)
            except:
                self._logger.exception('Un-registering service {0} on {1} failed'.format(service_name, node.ip))

        # Cleans up a complete cluster (remove services, directories and configuration files)
        for node in self.config.nodes:
            self._destroy_node(node=node, delay_unregistration=self.config.source_ip is not None)
            self.config.delete_config()
        self._logger.debug('Deleting cluster {0} completed'.format(self.cluster_name))

    def extend_cluster(self, new_ip, base_dir, log_sinks, crash_log_sinks, plugins=None, locked=True, port_range=None, preferred_master=False):
        """
        Extends a cluster to a given new node
        :param new_ip: IP address of the node to be added
        :type new_ip: str
        :param base_dir: Base directory that should contain the data and tlogs
        :type base_dir: str
        :param log_sinks: Logsink
        :type log_sinks: str
        :param crash_log_sinks: Logsink for crash reports
        :type crash_log_sinks: str
        :param plugins: Plugins that should be added to the configuration file
        :type plugins: dict
        :param locked: Indicates whether the extend should run in a locked context (e.g. to prevent port conflicts)
        :type locked: bool
        :param port_range: Range of ports which should be used for the Arakoon processes (2 available ports in the range will be selected) eg: [26400, 26499]
        :type port_range: list
        :param preferred_master: Indicate this node as 1 of the preferred masters during master election
        :type preferred_master: bool
        """
        self._logger.debug('Extending cluster {0} to {1}'.format(self.cluster_name, new_ip))
        if self.config is None:
            raise RuntimeError('Config not yet loaded')
        client = SSHClient(endpoint=new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        base_dir = base_dir.rstrip('/')
        home_dir = ArakoonInstaller.ARAKOON_HOME_DIR.format(base_dir, self.cluster_name)
        tlog_dir = ArakoonInstaller.ARAKOON_TLOG_DIR.format(base_dir, self.cluster_name)
        node_name = self._system.get_my_machine_id(client=client)
        self.clean_leftover_arakoon_data(ip=new_ip, directories=[home_dir, tlog_dir])
        if self.config.plugins is not None:
            for plugin in self.config.plugins:
                if plugins is None or plugin not in plugins:
                    raise RuntimeError('The plugins should be equal to all nodes')

        port_mutex = None
        try:
            if locked is True:
                volatile_mutex = self._get_volatile_mutex()
                port_mutex = volatile_mutex('arakoon_install_ports_{0}'.format(new_ip))
                port_mutex.acquire(wait=60)

            if self.is_filesystem is True:
                if port_range is None:
                    port_range = [26400]
                ports = self._system.get_free_ports(selected_range=port_range, nr=2, client=client)
            else:
                ports = self._get_free_ports(client=client, port_range=port_range)

            if node_name not in [node.name for node in self.config.nodes]:
                self.config.nodes.append(ArakoonNodeConfig(name=node_name,
                                                           ip=new_ip,
                                                           client_port=ports[0],
                                                           messaging_port=ports[1],
                                                           log_sinks=log_sinks,
                                                           crash_log_sinks=crash_log_sinks,
                                                           home=home_dir,
                                                           tlog_dir=tlog_dir,
                                                           preferred_master=preferred_master))
            self._deploy(plugins=plugins.values() if plugins is not None else None,
                         delay_service_registration=self.is_filesystem)
        finally:
            if port_mutex is not None:
                port_mutex.release()

        self._logger.debug('Extending cluster {0} to {1} completed'.format(self.cluster_name, new_ip))

    def shrink_cluster(self, removal_ip, offline_nodes=None):
        """
        Removes a node from a cluster, the old node will become a slave
        :param removal_ip: The IP of the node that should be removed from the cluster
        :type removal_ip: str
        :param offline_nodes: Storage Routers which are offline
        :type offline_nodes: list
        :return: None
        :rtype: NoneType
        """
        if offline_nodes is None:
            offline_nodes = []

        self._logger.debug('Shrinking cluster {0} from {1}'.format(self.cluster_name, removal_ip))
        if self.config is None:
            raise RuntimeError('Config not yet loaded')
        removal_node = None
        for node in self.config.nodes[:]:
            if node.ip == removal_ip:
                if node.name in self.config.export_dict()['global'].get('preferred_masters', '').split(','):
                    self._logger.warning('OVS_WARNING: Preferred master node {0} has been removed from cluster {1}'.format(node.name, self.cluster_name))

                self.config.nodes.remove(node)
                removal_node = node
                if node.ip not in offline_nodes:
                    self._destroy_node(node=node,
                                       delay_unregistration=self.is_filesystem)
                    if self.is_filesystem is True:
                        self.config.delete_config(removal_ip)
                break

        if removal_node is not None:
            self._deploy(offline_nodes=offline_nodes,
                         delay_service_registration=self.is_filesystem)
            self._service_manager.unregister_service(node_name=removal_node.name,
                                                     service_name=self.get_service_name_for_cluster(cluster_name=self.cluster_name))
        self._logger.debug('Shrinking cluster {0} from {1} completed'.format(self.cluster_name, removal_ip))

    @classmethod
    def clean_leftover_arakoon_data(cls, ip, directories):
        """
        Delete existing Arakoon data
        :param ip: IP on which to check for existing data
        :type ip: str
        :param directories: Directories to delete
        :type directories: list
        :return: None
        :rtype: NoneType
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return

        logger = cls._get_logger_instance()
        root_client = SSHClient(ip, username='root')

        # Verify whether all files to be archived have been released properly
        open_file_errors = []
        logger.debug('Cleanup old Arakoon - Checking open files')
        dirs_with_files = {}
        for directory in directories:
            logger.debug('Cleaning old Arakoon - Checking directory {0}'.format(directory))
            if root_client.dir_exists(directory):
                logger.debug('Cleaning old Arakoon - Directory {0} exists'.format(directory))
                file_names = root_client.file_list(directory, abs_path=True, recursive=True)
                if len(file_names) > 0:
                    logger.debug('Cleaning old Arakoon - Files found in directory {0}'.format(directory))
                    dirs_with_files[directory] = file_names
                for file_name in file_names:
                    try:
                        open_files = root_client.run(['lsof', file_name])
                        if open_files != '':
                            open_file_errors.append('Open file {0} detected in directory {1}'.format(os.path.basename(file_name), directory))
                    except CalledProcessError:
                        continue

        if len(open_file_errors) > 0:
            raise RuntimeError('\n - ' + '\n - '.join(open_file_errors))

        for directory, info in dirs_with_files.iteritems():
            logger.debug('Cleanup old Arakoon - Removing old files from {0}'.format(directory))
            root_client.file_delete(info)

    @classmethod
    def get_unused_arakoon_metadata_and_claim(cls, cluster_type, cluster_name=None):
        """
        Retrieve cluster information based on its type
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES)
        :type cluster_type: str
        :param cluster_name: Name of the cluster to claim
        :type cluster_name: str
        :return: Metadata of the cluster
        :rtype: dict
        """
        configuration = cls._get_configuration()
        if cluster_type not in ARAKOON_CLUSTER_TYPES:
            raise ValueError('Unsupported Arakoon cluster type provided. Please choose from {0}'.format(', '.join(sorted(ARAKOON_CLUSTER_TYPES))))
        if not configuration.dir_exists(ArakoonClusterConfig.CONFIG_ROOT):
            return None

        volatile_mutex = cls._get_volatile_mutex()
        mutex = volatile_mutex('claim_arakoon_metadata', wait=10)
        locked = cluster_type not in ['CFG', 'FWK']
        try:
            if locked is True:
                mutex.acquire()

            for cl_name in configuration.list(ArakoonClusterConfig.CONFIG_ROOT):
                if cluster_name is not None and cl_name != cluster_name:
                    continue
                config = ArakoonClusterConfig(cluster_id=cl_name, configuration=configuration)
                arakoon_client = cls.build_client(config)
                if arakoon_client.exists(ArakoonInstaller.METADATA_KEY):
                    metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
                    if metadata['cluster_type'] == cluster_type and metadata['in_use'] is False and metadata['internal'] is False:
                        metadata['in_use'] = True
                        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))
                        return metadata
        finally:
            if locked is True:
                mutex.release()

    @classmethod
    def get_unused_arakoon_clusters(cls, cluster_type):
        """
        Retrieve all unclaimed clusters of type <cluster_type>
        :param cluster_type: Type of the cluster (See ServiceType.ARAKOON_CLUSTER_TYPES w/o type CFG, since this is not available in the configuration management)
        :type cluster_type: str
        :return: All unclaimed clusters of specified type
        :rtype: list
        """
        clusters = []
        configuration = cls._get_configuration()
        if not configuration.dir_exists(ArakoonClusterConfig.CONFIG_ROOT):
            return clusters

        supported_types = ARAKOON_CLUSTER_TYPES[:]
        supported_types.remove('CFG')
        if cluster_type not in supported_types:
            raise ValueError('Unsupported Arakoon cluster type provided. Please choose from {0}'.format(', '.join(sorted(supported_types))))

        for cluster_name in configuration.list(ArakoonClusterConfig.CONFIG_ROOT):
            metadata = cls.get_arakoon_metadata_by_cluster_name(cluster_name=cluster_name)
            if metadata['cluster_type'] == cluster_type and metadata['in_use'] is False:
                clusters.append(metadata)
        return clusters

    @classmethod
    def get_arakoon_metadata_by_cluster_name(cls, cluster_name, ip=None):
        """
        Retrieve cluster information based on its name
        :param cluster_name: Name of the cluster
        :type cluster_name: str
        :param ip: The IP address of one of the nodes containing the configuration file (Only required for filesystem Arakoons)
        :type ip: str
        :return: Cluster metadata information
        :rtype: dict
        """
        configuration = cls._get_configuration()
        config = ArakoonClusterConfig(cluster_id=cluster_name, configuration=configuration, source_ip=ip)
        arakoon_client = cls.build_client(config)
        return json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))

    @classmethod
    def start(cls, cluster_name, client):
        """
        Starts a cluster service on the client provided
        :param cluster_name: The name of the cluster service to start
        :type cluster_name: str
        :param client: Client on which to start the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        service_manager = cls._get_service_manager()
        service_name = cls.get_service_name_for_cluster(cluster_name=cluster_name)
        if service_manager.has_service(name=service_name, client=client) is True:
            service_manager.start_service(name=service_name, client=client)

    @classmethod
    def stop(cls, cluster_name, client):
        """
        Stops a cluster service on the client provided
        :param cluster_name: The name of the cluster service to stop
        :type cluster_name: str
        :param client: Client on which to stop the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        service_manager = cls._get_service_manager()
        service_name = cls.get_service_name_for_cluster(cluster_name=cluster_name)
        if service_manager.has_service(name=service_name, client=client) is True:
            service_manager.stop_service(name=service_name, client=client)

    @classmethod
    def is_running(cls, cluster_name, client):
        """
        Checks if the cluster service is running on the client provided
        :param cluster_name: The name of the cluster service to check
        :type cluster_name: str
        :param client: Client on which to check the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: True if the cluster service is running, False otherwise
        :rtype: bool
        """
        service_manager = cls._get_service_manager()
        service_name = cls.get_service_name_for_cluster(cluster_name=cluster_name)
        if service_manager.has_service(name=service_name, client=client):
            return service_manager.get_service_status(name=service_name, client=client) == 'active'
        return False

    @classmethod
    def remove(cls, cluster_name, client, delay_unregistration=False):
        """
        Removes a cluster service from the client provided
        :param cluster_name: The name of the cluster service to remove
        :type cluster_name: str
        :param client: Client on which to remove the service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param delay_unregistration: Un-register the cluster service right away or not
        :type delay_unregistration: bool
        :return: None
        :rtype: NoneType
        """
        service_manager = cls._get_service_manager()
        service_name = cls.get_service_name_for_cluster(cluster_name=cluster_name)
        if service_manager.has_service(name=service_name, client=client) is True:
            service_manager.remove_service(name=service_name, client=client, delay_unregistration=delay_unregistration)

    def start_cluster(self):
        """
        Execute a start sequence (only makes sense for a fresh cluster)
        :return: None
        :rtype: NoneType
        """
        root_clients = [SSHClient(endpoint=node.ip, username='root') for node in self.config.nodes]
        for client in root_clients:
            self.start(cluster_name=self.cluster_name, client=client)
        self.store_config()

        self.metadata['in_use'] = True
        arakoon_client = self._wait_for_cluster()
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(self.metadata, indent=4))

    def restart_node(self, client):
        """
        Execute a restart sequence for the cluster service running on the specified client
        This scenario is only supported when NO configuration changes have been applied
        and should have no impact on Arakoon performance if 1 node fails to restart due to backwards compatibility
        :param client: Client on which to restart the cluster service
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: None
        :rtype: NoneType
        """
        self._logger.debug('Restarting node {0} for cluster {1}'.format(client.ip, self.cluster_name))
        if self.config is None:
            raise RuntimeError('Config not yet loaded')
        if len(self.config.nodes) > 0:
            self.stop(cluster_name=self.cluster_name, client=client)
            self.start(cluster_name=self.cluster_name, client=client)
            self._wait_for_cluster()
            self._logger.debug('Restarted node {0} on cluster {1}'.format(client.ip, self.cluster_name))

    def restart_cluster(self):
        """
        Execute a restart sequence for the specified cluster.
        :return: None
        :rtype: NoneType
        """
        self._logger.debug('Restarting cluster {0}'.format(self.cluster_name))
        if self.config is None:
            raise RuntimeError('Config not yet loaded')
        for node in self.config.nodes:
            self._logger.debug('  Restarting node {0} for cluster {1}'.format(node.ip, self.cluster_name))
            root_client = SSHClient(endpoint=node.ip, username='root')
            self.stop(cluster_name=self.cluster_name, client=root_client)
            self.start(cluster_name=self.cluster_name, client=root_client)
            self._logger.debug('  Restarted node {0} for cluster {1}'.format(node.ip, self.cluster_name))
            if len(self.config.nodes) >= 2:  # A two node cluster needs all nodes running
                self._wait_for_cluster()
        self._wait_for_cluster()

    def restart_cluster_after_extending(self, new_ip):
        """
        Execute a (re)start sequence after adding a new node to a cluster.
        :param new_ip: IP of the newly added node
        :type new_ip: str
        :return: None
        :rtype: NoneType
        """
        self._logger.debug('Restarting cluster {0} after adding node with IP {1}'.format(self.cluster_name, new_ip))
        if self.config is None:
            raise RuntimeError('Config not yet loaded')

        client = SSHClient(endpoint=new_ip, username=ArakoonInstaller.SSHCLIENT_USER)
        if self.is_running(cluster_name=self.cluster_name, client=client):
            self._logger.info('Arakoon service for {0} is already running'.format(self.cluster_name))
            return

        self._logger.debug('Catching up new node {0} for cluster {1}'.format(new_ip, self.cluster_name))
        node_name = [node.name for node in self.config.nodes if node.ip == new_ip][0]
        client.run(['arakoon', '--node', node_name, '-config', self.config.external_config_path, '-catchup-only'])
        self._logger.debug('Catching up new node {0} for cluster {1} completed'.format(new_ip, self.cluster_name))

        # Restart current nodes in the cluster
        for node in self.config.nodes:
            if node.ip == new_ip:
                continue
            self._logger.debug('  Restarting node {0} for cluster {1}'.format(node.ip, self.cluster_name))
            root_client = SSHClient(endpoint=node.ip, username='root')
            self.stop(cluster_name=self.cluster_name, client=root_client)
            self.start(cluster_name=self.cluster_name, client=root_client)
            self._logger.debug('  Restarted node {0} for cluster {1}'.format(node.ip, self.cluster_name))
            if len(self.config.nodes) >= 3:
                self._wait_for_cluster()

        # Start new node in the cluster
        client = SSHClient(endpoint=new_ip, username='root')
        self.start(cluster_name=self.cluster_name, client=client)
        self.store_config()
        self._logger.debug('Started node {0} for cluster {1}'.format(new_ip, self.cluster_name))

    def restart_cluster_after_shrinking(self):
        """
        Execute a restart sequence after removing a node from a cluster
        :return: None
        :rtype: NoneType
        """
        self._logger.debug('Restarting cluster {0} after shrinking'.format(self.cluster_name))
        for node in self.config.nodes:
            self._logger.debug('  Restarting node {0} for cluster {1}'.format(node.ip, self.cluster_name))
            root_client = SSHClient(endpoint=node.ip, username='root')
            self.stop(cluster_name=self.cluster_name, client=root_client)
            self.start(cluster_name=self.cluster_name, client=root_client)
            self._logger.debug('  Restarted node {0} for cluster {1}'.format(node.ip, self.cluster_name))
            if len(self.config.nodes) >= 2:  # A two node cluster needs all nodes running
                self._wait_for_cluster()

        self.store_config()

    def claim_cluster(self):
        """
        Claims the cluster
        :return: None
        :rtype: NoneType
        """
        arakoon_client = self.build_client(config=self.config)
        metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
        metadata['in_use'] = True
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))

    def unclaim_cluster(self):
        """
        Un-claims the cluster
        :return: None
        :rtype: NoneType
        """
        arakoon_client = self.build_client(config=self.config)
        metadata = json.loads(arakoon_client.get(ArakoonInstaller.METADATA_KEY))
        metadata['in_use'] = False
        arakoon_client.set(ArakoonInstaller.METADATA_KEY, json.dumps(metadata, indent=4))

    def store_config(self):
        """
        Stores the configuration inside the cluster
        """
        arakoon_client = self._wait_for_cluster()
        arakoon_client.set(ArakoonInstaller.INTERNAL_CONFIG_KEY, self.config.export_ini())

    @classmethod
    def build_client(cls, config):
        """
        Build the ArakoonClient object with all configured nodes in the cluster
        :param config: Configuration on which to base the client
        :type config: ArakoonClientConfig
        :return: The newly generated PyrakoonClient
        :rtype: PyrakoonClient
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            from ovs_extensions.db.arakoon.tests.client import MockPyrakoonClient
            return MockPyrakoonClient(config.cluster_id, None)

        from ovs_extensions.db.arakoon.pyrakoon.client import PyrakoonClient
        nodes = {}
        for node in config.nodes:
            nodes[node.name] = ([node.ip], node.client_port)
        return PyrakoonClient(config.cluster_id, nodes)

    @classmethod
    def get_service_name_for_cluster(cls, cluster_name):
        """
        Retrieve the Arakoon service name for the cluster specified
        :param cluster_name: Name of the Arakoon cluster
        :type cluster_name: str
        :return: Name of the Arakoon service known on the system
        :rtype: str
        """
        return 'arakoon-{0}'.format(cluster_name)

    def get_log_sink_path(self):
        """
        Retrieve the sink path for log records generated by Arakoon to sink to
        :return: The sink path
        :rtype: str
        """
        return self._logger.get_sink_path('arakoon-server_{0}'.format(self.cluster_name))

    def get_crash_log_sink_path(self):
        """
        Retrieve the sink path for crash log records generated by Arakoon to sink to
        :return: The sink path
        :rtype: str
        """
        return self._logger.get_sink_path('arakoon-server-crash_{0}'.format(self.cluster_name))

    ################
    # HELPER METHODS
    def _wait_for_cluster(self):
        """
        Waits for an Arakoon cluster to be available (by sending a nop)
        """
        self._logger.debug('Waiting for cluster {0}'.format(self.cluster_name))
        arakoon_client = self.build_client(self.config)
        arakoon_client.nop()
        self._logger.debug('Waiting for cluster {0}: available'.format(self.cluster_name))
        return arakoon_client

    def _get_free_ports(self, client, port_range=None):
        node_name = self._system.get_my_machine_id(client)
        clusters = []
        exclude_ports = []
        if self._configuration.dir_exists(ArakoonClusterConfig.CONFIG_ROOT):
            for cluster_name in self._configuration.list(ArakoonClusterConfig.CONFIG_ROOT):
                config = ArakoonClusterConfig(cluster_id=cluster_name, configuration=self._configuration)
                for node in config.nodes:
                    if node.name == node_name:
                        clusters.append(cluster_name)
                        exclude_ports.append(node.client_port)
                        exclude_ports.append(node.messaging_port)

        if port_range is None:
            port_range = self._configuration.get('/ovs/framework/hosts/{0}/ports|arakoon'.format(node_name))
        ports = self._system.get_free_ports(selected_range=port_range, exclude=exclude_ports, nr=2, client=client)
        self._logger.debug('  Loaded free ports {0} based on existing clusters {1}'.format(ports, clusters))
        return ports

    def _destroy_node(self, node, delay_unregistration=False):
        """
        Cleans up a single node (remove services, directories and configuration files)
        """
        self._logger.debug('Destroy node {0} in cluster {1}'.format(node.ip, self.cluster_name))

        # Removes services for a cluster on a given node
        root_client = SSHClient(node.ip, username='root')
        self.stop(cluster_name=self.cluster_name, client=root_client)
        self.remove(cluster_name=self.cluster_name, client=root_client, delay_unregistration=delay_unregistration)

        # Cleans all directories on a given node
        abs_paths = {node.tlog_dir, node.home}  # That's a set
        if node.log_sinks.startswith('/'):
            abs_paths.add(os.path.dirname(os.path.abspath(node.log_sinks)))
        if node.crash_log_sinks.startswith('/'):
            abs_paths.add(os.path.dirname(os.path.abspath(node.crash_log_sinks)))
        root_client.dir_delete(list(abs_paths))
        self._logger.debug('Destroy node {0} in cluster {1} completed'.format(node.ip, self.cluster_name))

    def _deploy(self, offline_nodes=None, plugins=None, delay_service_registration=False):
        """
        Deploys a complete cluster: Distributing the configuration files, creating directories and services
        """
        self._logger.debug('Deploying cluster {0}'.format(self.config.cluster_id))
        if offline_nodes is None:
            offline_nodes = []

        self.service_metadata = {}
        for node in self.config.nodes:
            if node.ip in offline_nodes:
                continue
            self._logger.debug('  Deploying cluster {0} on {1}'.format(self.cluster_name, node.ip))
            root_client = SSHClient(node.ip, username='root')

            # Distributes a configuration file to all its nodes
            self.config.write_config(ip=node.ip)

            # Create dirs as root because mountpoint /mnt/cache1 is typically owned by root
            abs_paths = {node.tlog_dir, node.home}  # That's a set
            if node.log_sinks.startswith('/'):
                abs_paths.add(os.path.dirname(os.path.abspath(node.log_sinks)))
            if node.crash_log_sinks.startswith('/'):
                abs_paths.add(os.path.dirname(os.path.abspath(node.crash_log_sinks)))
            abs_paths = list(abs_paths)
            root_client.dir_create(abs_paths)
            root_client.dir_chmod(abs_paths, 0755, recursive=True)
            root_client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

            # Creates services for/on all nodes in the config
            metadata = None
            if self.config.source_ip is None:
                configuration_key = self._service_manager.service_config_key.format(self._system.get_my_machine_id(root_client),
                                                                                    self.get_service_name_for_cluster(cluster_name=self.cluster_name))
                # If the entry is stored in arakoon, it means the service file was previously made
                if self._configuration.exists(configuration_key):
                    metadata = self._configuration.get(configuration_key)
            if metadata is None:
                extra_version_cmd = ''
                if plugins is not None:
                    extra_version_cmd = ';'.join(plugins)
                    extra_version_cmd = extra_version_cmd.strip(';')
                metadata = self._service_manager.add_service(name='ovs-arakoon',
                                                             client=root_client,
                                                             params={'CLUSTER': self.cluster_name,
                                                                     'NODE_ID': node.name,
                                                                     'CONFIG_PATH': self.config.external_config_path,
                                                                     'EXTRA_VERSION_CMD': extra_version_cmd},
                                                             target_name='ovs-arakoon-{0}'.format(self.cluster_name),
                                                             startup_dependency=('ovs-watcher-config' if self.config.source_ip is None else None),
                                                             delay_registration=delay_service_registration)
            self.service_metadata[node.ip] = metadata
            self._logger.debug('  Deploying cluster {0} on {1} completed'.format(self.cluster_name, node.ip))

    @classmethod
    def _get_configuration(cls):
        raise NotImplementedError()

    @classmethod
    def _get_service_manager(cls):
        raise NotImplementedError()

    @classmethod
    def _get_system(cls):
        raise NotImplementedError()

    @classmethod
    def _get_volatile_mutex(cls):
        raise NotImplementedError()

    @classmethod
    def _get_logger_instance(cls):
        return Logger('extensions-db')

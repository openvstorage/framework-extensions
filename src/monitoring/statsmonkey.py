# Copyright (C) 2017 iNuron NV
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
Statsmonkey module responsible for retrieving certain statistics from the cluster and send them to an Influx DB or Redis DB
Classes: StatsMonkey
"""

import copy
import math
import time
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from threading import Thread


class StatsMonkey(object):
    """
    Stats Monkey class which executes all 'get_stats_' methods of the inheriting class
    """
    _logger = None  # Filled out by the inheriting class
    _config = None  # Config is validated once during 'run_all_get_stat_methods'
    _stats_client = None  # Client gets initialized once during 'run_all_get_stat_methods'
    _dynamic_dependencies = {}  # Filled out by the inheriting class

    _default_interval = 60

    @classmethod
    def _get_configuration(cls):
        raise NotImplementedError()

    @classmethod
    def run_all_get_stat_methods(cls):
        """
        Stats Monkey class which executes all 'get_stats_' methods of the inheriting class
        Prerequisites when adding content:
            * New methods which need to be picked up by this method need to start with 'get_stats_'
            * New methods need to collect the information and return a bool and list of stats. Then 'run_all_get_stat_methods' method, will send the stats to the configured instance (influx / redis)
            * The frequency each method needs to be executed can be configured via the configuration management by setting the function name as key and the interval in seconds as value
            *    Eg: {'get_stats_vpools': 20}  --> Every 20 seconds, the vpool statistics will be checked upon
        :return: None
        :rtype: NoneType
        """
        def run_method(name, errors):
            """
            Execute the method, catch potential errors and send the statistics to the configured instance (influx / redis)
            """
            cls._logger.info("Executing function '{0}'".format(name))
            stats = []
            try:
                errored, stats = getattr(cls, name)()
                cls._logger.debug("Executed function '{0}'".format(name))
            except Exception:
                errored = True
                cls._logger.exception("Executing function '{0}' failed".format(name))

            if len(stats) == 0:
                cls._logger.debug("No statistics found for function '{0}'".format(name))
            else:
                try:
                    cls._logger.debug("Sending statistics for function '{0}'".format(name))
                    cls._send_stats(stats=stats, name=name)
                except Exception:
                    errored = True
                    cls._logger.exception("Sending statistics for function '{0}' failed".format(name))

            if errored is True:
                errors.append(name)

        def schedule_method(name, interval, errors):
            """
            Make sure each method is executed every 60 / interval seconds
            """
            run_times = int(math.floor(cls._default_interval / interval))  # Scheduled task runs every minute, so we verify how many times each function has to be executed this minute
            for counter in range(run_times):
                if name in [thr.name for thr in function_threads if thr.is_alive() is True]:
                    cls._logger.debug("Function '{0}' is still processing, skipping this iteration".format(name))
                    if counter < run_times - 1:
                        time.sleep(interval)
                    continue

                function_thread = Thread(target=run_method, name=name, args=(name, errors))
                function_thread.start()
                function_threads.append(function_thread)
                if counter < run_times - 1:
                    cls._logger.debug("Function '{0}' was launched, now waiting {1}s for another execution".format(name, interval))
                    time.sleep(interval)

        def validate_interval(name, interval):
            """
            Validate whether the dynamic properties which are used in the get_stats_method are invalidated more frequently than the configured interval of the get_stats_method
            Eg: vpool.statistics is removed from cache every 30s in the DAL
                If the interval would be configured by setting get_stats_vpools: 20, then its useless to have it checked more frequently than the model updates this value
            """
            for klass, dynamic_names in cls._dynamic_dependencies.get(name, {}).iteritems():
                # noinspection PyProtectedMember
                dynamic_timeouts = dict((d.name, d.timeout) for d in klass._dynamics)
                for dynamic_name in dynamic_names:
                    if dynamic_name not in dynamic_timeouts:
                        cls._logger.error("Dynamic property '{0}' does not exist for class '{1}'".format(dynamic_name, klass.__name__))
                        continue
                    if interval < dynamic_timeouts[dynamic_name]:
                        cls._logger.warning("Dynamic property '{0}' invalidates every {1}s, but its executing interval is configured at {2}".format(dynamic_name, dynamic_timeouts[dynamic_name], interval))

        cls._logger.info('Started')
        cls._logger.debug('Verifying configuration')
        config = cls.validate_and_retrieve_config()

        cls._logger.debug('Verifying package installation for configured transport type')
        cls._create_client()

        method_names = [method for method in dir(cls) if method.startswith('get_stats_') and callable(getattr(cls, method))]
        cls._default_interval = config.get('interval', cls._default_interval)
        function_threads = list()
        errored_functions = list()
        scheduler_threads = list()
        method_interval_map = dict((method_name, config.get(method_name, cls._default_interval)) for method_name in method_names)
        for method_name, method_interval in method_interval_map.iteritems():
            if method_interval > cls._default_interval:
                cls._logger.warning('Method {0} is scheduled to run every {1}s which is larger than the global interval. Switching to interval of {2}s'.format(method_name, method_interval, cls._default_interval))
                method_interval = cls._default_interval
            else:
                cls._logger.info('Method {0} is scheduled to run every {1}s'.format(method_name, method_interval))
            validate_interval(name=method_name, interval=method_interval)
            scheduler_thread = Thread(target=schedule_method, args=(method_name, method_interval, errored_functions))
            scheduler_thread.start()
            scheduler_threads.append(scheduler_thread)

        # First wait threads which verify how many times each function needs to be executed (scheduler)
        for thread in scheduler_threads:
            thread.join()
        # Then wait for the threads actually executing the functions
        for thread in function_threads:
            thread.join()

        if len(errored_functions) > 0:
            raise Exception('StatsMonkey failed to retrieve statistics in following functions:\n * {0}'.format('\n * '.join(sorted(errored_functions))))
        cls._logger.info('Stats Monkey has fulfilled its job')

    @classmethod
    def validate_and_retrieve_config(cls):
        """
        Retrieve and validate the configuration for StatsMonkey
        :return: The configuration set at /ovs/framework/monitoring/stats_monkey
        :rtype: dict
        """
        config_key = '/ovs/framework/monitoring/stats_monkey'
        config = cls._get_configuration()
        if not config.exists(config_key):
            raise ValueError('StatsMonkey requires a configuration key at {0}'.format(config_key))

        config = config.get(config_key)
        if not isinstance(config, dict):
            raise ValueError('StatsMonkey configuration must be of type dict')

        required_params = {'host': (str, ExtensionsToolbox.regex_ip),
                           'port': (int, {'min': 1025, 'max': 65535}),
                           'interval': (int, {'min': 1}, False),
                           'database': (str, None),
                           'transport': (str, ['influxdb', 'redis', 'graphite']),
                           'environment': (str, None)}
        if config.get('transport') == 'influxdb':
            required_params['username'] = (str, None)
        if config.get('transport') in ['influxdb', 'reddis']:
            required_params['password'] = (str, None)

        ExtensionsToolbox.verify_required_params(actual_params=config, required_params=required_params)
        cls._config = config
        return cls._config

    @classmethod
    def _pop_realtime_info(cls, points):
        for key, value in copy.deepcopy(points).iteritems():
            if key.endswith('_ps'):
                points.pop(key)
        return points

    @classmethod
    def _convert_to_float_values(cls, json_output, prefix=None):
        if prefix is None:
            prefix = []
        output = {}
        for key in json_output.keys():
            path = prefix + [key]
            if isinstance(json_output[key], dict):
                output.update(cls._convert_to_float_values(json_output[key], path))
            else:
                output['_'.join(str(k) for k in path)] = float(json_output[key])
        return output

    @classmethod
    def _send_stats(cls, stats, name):
        # New client since the configuration can change in meantime
        stats_client = cls._create_client()
        if cls._config['transport'] == 'graphite':
            stats_client.send_statsmonkey_data(stats, name)
        elif cls._config['transport'] == 'influxdb':
            stats_client.write_points(stats)
        else:
            stats_client.lpush(cls._config['database'], stats)

    @classmethod
    def _create_client(cls):
        # type: () -> any
        """
        Validate whether the correct imports can be done to create a client based on the 'transport' specified in the configuration
        """
        host = cls._config['host']
        port = cls._config['port']
        user = cls._config.get('username')
        password = cls._config.get('password')
        database = cls._config['database']
        if cls._config['transport'] == 'graphite':
            from ovs.extensions.generic.graphiteclient import GraphiteClient
            stats_client = GraphiteClient(host, port, database)
        elif cls._config['transport'] == 'influxdb':
            import influxdb
            stats_client = influxdb.InfluxDBClient(host=host, port=port, username=user, password=password, database=database)
        else:
            import redis
            stats_client = redis.Redis(host=host, port=port, password=password)
        return stats_client

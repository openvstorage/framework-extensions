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
Contains the LogHandler module
"""

import os
import sys
import time
import socket
import inspect
import logging
import itertools


class LogFormatter(logging.Formatter):
    """
    Formatter for the logger
    """
    def formatTime(self, record, datefmt=None):
        """
        Overrides the default formatter to include UTC offset
        """
        _ = datefmt
        ct = self.converter(record.created)
        tz = time.altzone if time.daylight and ct.tm_isdst > 0 else time.timezone
        offset = '{0}{1:0>2}{2:0>2}'.format('-' if tz > 0 else '+', abs(tz) // 3600, abs(tz // 60) % 60)
        base_time = time.strftime('%Y-%m-%d %H:%M:%S', ct)
        return '{0} {1:03.0f}00 {2}'.format(base_time, record.msecs, offset)

    def format(self, record):
        """
        Format a record
        :param record: Record to format
        :return: Formatted record
        """
        if 'hostname' not in record.__dict__:
            record.hostname = socket.gethostname()
        if 'sequence' not in record.__dict__:
            record.sequence = LogHandler.counter.next()
        return super(LogFormatter, self).format(record)


class LogHandler(object):
    """
    Log handler.

    WARNING: This log handler might be highly unreliable if not used correctly. It can log to redis, but if Redis is
    not working as expected, it will result in lost log messages. If you want reliable logging, do not use Redis at all
    or log to files and have a separate process forward them to Redis (so logs can be re-send if Redis is unavailable)
    """
    TARGET_TYPE_FILE = 'file'
    TARGET_TYPE_REDIS = 'redis'
    TARGET_TYPE_CONSOLE = 'console'

    LOG_PATH = None
    LOG_LEVELS = {10: 'DEBUG',
                  20: 'INFO',
                  30: 'WARNING',
                  40: 'ERROR',
                  50: 'CRITICAL'}
    TARGET_TYPES = [TARGET_TYPE_FILE, TARGET_TYPE_REDIS, TARGET_TYPE_CONSOLE]
    DEFAULT_LOG_LEVEL = 'DEBUG'
    DEFAULT_TARGET_TYPE = TARGET_TYPE_CONSOLE

    counter = itertools.count()
    _logs = {}  # Used by unittests
    _cache = {}
    _propagate_cache = {}

    def __init__(self, source, name, propagate, target_type):
        """
        Initializes the logger
        """
        parent_invoker = inspect.stack()[1]
        if not __file__.startswith(parent_invoker[1]) or parent_invoker[3] != 'get':
            raise RuntimeError('Cannot invoke instance from outside this class. Please use LogHandler.get(source, name=None) instead')

        name = 'logger' if name is None else name
        self._key = '{0}_{1}'.format(source, name)
        self._target_type = target_type
        self._unittest_mode = os.environ.get('RUNNING_UNITTESTS') == 'True'

        self.logger = logging.getLogger(self._key)
        self.logger.propagate = propagate

    @classmethod
    def get(cls, source, name=None, propagate=False, forced_target_type=None):
        """
        Retrieve a logging.getLogger instance.
        WARNING: This is not how python logging should be handled. Each process making use of a logger should create its own instance (logging.getLogger(__name__))
                 and this class should only provide the handler (file, stream, redis, ...)
        """
        try:
            logging_info = cls.get_logging_info()
        except:
            logging_info = {}

        try:
            target_definition = cls._load_target_definition(source, allow_override=True, forced_target_type=forced_target_type)
        except:
            target_definition = {}

        level = logging_info.get('level', cls.DEFAULT_LOG_LEVEL).upper()
        target_type = target_definition.get('type', cls.DEFAULT_TARGET_TYPE)
        if level not in cls.LOG_LEVELS.values():
            raise ValueError('Invalid log level specified: {0}'.format(level))
        if target_type not in cls.TARGET_TYPES:
            raise ValueError('Invalid target type specified: {0}'.format(target_type))

        key = '{0}_{1}'.format(source, name)
        if key in cls._cache:
            logger = cls._cache[key]
            if logger.get_level()[1] != level:
                cls._cache.pop(key)
            elif logger.get_target_type() != target_type:
                cls._cache.pop(key)

        if key not in cls._cache:
            # Create handler
            if target_type == cls.TARGET_TYPE_REDIS:
                from redis import Redis
                from ovs_extensions.log.redis_logging import RedisListHandler
                handler = RedisListHandler(queue=target_definition['queue'],
                                           client=Redis(host=target_definition['host'],
                                                        port=target_definition['port']))
            elif target_type == cls.TARGET_TYPE_FILE:
                handler = logging.FileHandler(target_definition['filename'])
            else:
                handler = logging.StreamHandler(sys.stdout)

            # Add formatter to handler
            handler.setFormatter(LogFormatter('%(asctime)s - %(hostname)s - %(process)s/%(thread)d - {0}/%(name)s - %(sequence)s - %(levelname)s - %(message)s'.format(source)))

            logger = LogHandler(source=source,
                                name=name,
                                propagate=propagate,
                                target_type=target_type)
            logger.logger.setLevel(getattr(logging, level))
            for old_handler in logger.logger.handlers:  # Remove previously configured handlers in case a new target type would be defined
                logger.logger.removeHandler(hdlr=old_handler)
            logger.logger.addHandler(handler)
            cls._cache[key] = logger

        if key not in cls._propagate_cache:
            cls._propagate_cache[key] = propagate

        return cls._cache[key]

    @classmethod
    def get_sink_path(cls, source, allow_override=False, forced_target_type=None):
        """
        Retrieve the path to sink logs to
        :param source: Source
        :type source: str
        :param allow_override: Allow override
        :type allow_override: bool
        :param forced_target_type: Override target type
        :type forced_target_type: str
        :return: The path to sink to
        :rtype: str
        """
        target_definition = cls._load_target_definition(source, allow_override, forced_target_type)
        if target_definition['type'] == cls.TARGET_TYPE_CONSOLE:
            return 'console:'
        elif target_definition['type'] == cls.TARGET_TYPE_FILE:
            return target_definition['filename']
        elif target_definition['type'] == cls.TARGET_TYPE_REDIS:
            return 'redis://{0}:{1}{2}'.format(target_definition['host'], target_definition['port'], target_definition['queue'])
        else:
            raise ValueError('Invalid target type specified')

    @classmethod
    def load_path(cls, source):
        """
        Load path
        :param source: Source
        :return: Path
        """
        if cls.LOG_PATH is None:
            raise ValueError('LOG_PATH is not specified')

        log_filename = '{0}/{1}.log'.format(cls.LOG_PATH, source)
        if not os.path.exists(cls.LOG_PATH):
            os.mkdir(cls.LOG_PATH, 0777)
        if not os.path.exists(log_filename):
            open(log_filename, 'a').close()
            os.chmod(log_filename, 0o666)
        return log_filename

    @classmethod
    def get_logging_info(cls):
        """
        Retrieve logging information from the Configuration management
        Should be inherited
        """
        raise NotImplementedError()

    def get_level(self):
        """
        Retrieve the currently configured log level
        :return: The log level information
        :rtype: list
        """
        level_int = self.logger.getEffectiveLevel()
        level_string = LogHandler.LOG_LEVELS.get(level_int, 'NOT_SET')
        return [level_int, level_string]

    def set_level(self, level):
        """
        Set a log level for the current logger instance
        :param level: Level to set (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        :type level:
        :return: None
        :rtype: NoneType
        """
        level = level.upper()
        if level not in LogHandler.LOG_LEVELS.values():
            raise ValueError('Invalid log level specified: {0}'.format(level))
        self.logger.setLevel(getattr(logging, level))

    def get_target_type(self):
        """
        Retrieve the currently configured target type (file, console, redis)
        :return: The target type
        :rtype: str
        """
        return self._target_type

    def debug(self, msg, *args, **kwargs):
        """ Debug (log level 10) """
        return self._log(msg, 'debug', *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """ Info (log level 20) """
        return self._log(msg, 'info', *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """ Warning (log level 30) """
        return self._log(msg, 'warning', *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """ Error (log level 40) """
        return self._log(msg, 'error', *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        """ Exception (log level 40) """
        return self._log(msg, 'exception', *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """ Critical (log level 50) """
        return self._log(msg, 'critical', *args, **kwargs)

    def _log(self, msg, severity, *args, **kwargs):
        """
        Log pass-through
        """
        if self._unittest_mode is True:
            if self._key not in LogHandler._logs:
                LogHandler._logs[self._key] = {}
            LogHandler._logs[self._key][msg.strip()] = severity

        propagate = LogHandler._propagate_cache.get(self._key, None)
        if propagate is not None:
            self.logger.propagate = propagate

        if 'print_msg' in kwargs:
            del kwargs['print_msg']
            print msg
        extra = kwargs.get('extra', {})
        extra['hostname'] = socket.gethostname()
        extra['sequence'] = LogHandler.counter.next()
        kwargs['extra'] = extra
        try:
            return getattr(self.logger, severity)(msg, *args, **kwargs)
        except:
            pass

    @classmethod
    def _load_target_definition(cls, source, allow_override=False, forced_target_type=None):
        """
        Load the logger target
        :param source: Source
        :type source: str
        :param allow_override: Allow override
        :type allow_override: bool
        :param forced_target_type: Override target type
        :type forced_target_type: str
        :return: Target definition
        :rtype: dict
        """
        target_type = cls.DEFAULT_TARGET_TYPE
        if allow_override is True:
            if 'OVS_LOGTYPE_OVERRIDE' in os.environ:
                target_type = os.environ['OVS_LOGTYPE_OVERRIDE']
            if forced_target_type is not None:
                target_type = forced_target_type

        if target_type == cls.TARGET_TYPE_REDIS:
            logging_target = cls.get_logging_info()
            queue = logging_target.get('queue', '/ovs/logging')
            if '{0}' in queue:
                queue = queue.format(source)
            return {'type': cls.TARGET_TYPE_REDIS,
                    'queue': '/{0}'.format(queue.lstrip('/')),
                    'host': logging_target.get('host', 'localhost'),
                    'port': logging_target.get('port', 6379)}
        elif target_type == cls.TARGET_TYPE_FILE:
            return {'type': cls.TARGET_TYPE_FILE,
                    'filename': cls.load_path(source)}
        else:
            return {'type': cls.TARGET_TYPE_CONSOLE}

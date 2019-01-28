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
This package contains logging modules
"""

from __future__ import absolute_import

import time
import socket
import logging
import itertools
import logging.config
from ..constants.logging import EXTENSIONS_LOGGER_NAME, LOG_FORMAT


class LogFormatter(logging.Formatter):
    """
    Formatter for the logger
    """

    # Counter to keep track of the sequence
    counter = itertools.count()

    def formatTime(self, record, datefmt=None):
        """
        Overrides the default formatter to include UTC offset. Is only called for when the formatter has %(asctime)s in it
        :param record: Record to format
        :type record: logging.LogRecord
        :param datefmt: Date format to apply to the record. If omitted, ISO8601 is used
        :type datefmt: str
        :return: The formatted timestamp
        :rtype: str
        """
        _ = datefmt
        ct = self.converter(record.created)
        tz = time.altzone if time.daylight and ct.tm_isdst > 0 else time.timezone
        offset = '{0}{1:0>2}{2:0>2}'.format('-' if tz > 0 else '+', abs(tz) // 3600, abs(tz // 60) % 60)
        base_time = time.strftime('%Y-%m-%d %H:%M:%S', ct)
        return '{0} {1:03.0f}00 {2}'.format(base_time, record.msecs, offset)

    def format(self, record):
        """
        Format a LogRecord
        :param record: Record to format
        :type record: logging.LogRecord
        :return: Formatted record
        :rtype: str
        """
        if 'hostname' not in record.__dict__:
            record.hostname = socket.gethostname()
        if 'sequence' not in record.__dict__:
            record.sequence = self.counter.next()
        return super(LogFormatter, self).format(record)


OVS_FORMATTER_CONFIG = {'()': LogFormatter.__module__ + '.' + LogFormatter.__name__,
                        'format': LOG_FORMAT}

DEFAULT_LOG_CONFIG = {'version': 1,
                      'disable_existing_loggers': False,
                      'formatters': {
                          'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'},
                          'ovs': OVS_FORMATTER_CONFIG
                      },
                      'handlers': {'default': {'level': 'INFO',
                                               'class': 'logging.StreamHandler',
                                               'formatter': 'ovs'}},
                      'loggers': {EXTENSIONS_LOGGER_NAME: {'handlers': ['default'],
                                                           'level': 'INFO',
                                                           'propagate': True},
                                  'urllib3': {'level': 'WARNING'},
                                  'requests': {'level': 'WARNING'},
                                  'paramiko': {'level': 'WARNING'}}
                      }


def get_extensions_logger():
    """
    Get the logger of the extensions
    All other loggers inherit from this logger
    """
    return logging.getLogger(EXTENSIONS_LOGGER_NAME)


def configure_logger_with_recommended_settings():
    """
    Configure the extensions logger with the recommended config
    """
    logging.config.dictConfig(DEFAULT_LOG_CONFIG)

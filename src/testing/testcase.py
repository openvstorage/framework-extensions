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

"""
Contains unittest testcases
The LogTestCase backports the assertLogs from python 3.4
"""

import logging
import unittest
import collections
from ..constants.logging import LOG_FORMAT_UNITTEST

# Create a tuple with 'records' and 'output' as fields. This guarantees immutability
_LoggingWatcherBase = collections.namedtuple("_LoggingWatcher", ["records", "output"])


class _LoggingWatcher(_LoggingWatcherBase):
    """
    Extends the named tuple
    """

    def get_message_severity_map(self):
        # type: () -> Dict[str, str]
        """
        Return the message - severity map
        All messages are keys, the value is the severity
        Created in order to not do too much changes in the unittests
        - The dict returned is the same as the old unittest logger dict
        :return: Dict with stripped messages as keys, stringified logging level as value
        :rtype: Dict[str, str]
        """
        message_severity_map = {}
        for record in self.records:  # type: logging.LogRecord
            message_severity_map[record.msg.strip()] = record.levelname
        return message_severity_map


class _CapturingHandler(logging.Handler):
    """
    A logging handler capturing all (raw and formatted) logging output.
    """

    def __init__(self):
        # type: () -> None
        """
        Initialize a capturing handler. This handler adds every record into the watcher
        """
        logging.Handler.__init__(self)
        self.watcher = _LoggingWatcher([], [])

    def flush(self):
        # type: () -> None
        """
        Ensure all logging output has been flushed.
        No need to implement with the capturing handler
        """
        pass

    def emit(self, record):
        # type: (logging.LogRecord) -> None
        """
        Do whatever it takes to actually log the specified logging record
        :param record: Record to log
        :return: None
        """
        self.watcher.records.append(record)
        msg = self.format(record)
        self.watcher.output.append(msg)


class _BaseTestCaseContext(object):

    def __init__(self, test_case):
        # type: (unittest.TestCase) -> None
        """
        Instantiate the TestCaseContext
        :param test_case: TestCase to work with
        """
        self.msg = None
        self.test_case = test_case

    def _raise_failure(self, message):
        # type: (str) -> None
        """
        Raise a failure message
        :param message: Message to raise
        """
        msg = self.test_case._formatMessage(self.msg, message)
        raise self.test_case.failureException(msg)


class _AssertLogsContext(_BaseTestCaseContext):
    """
    A context manager used to implement TestCase.assertLogs().
    """

    def __init__(self, test_case, logger_name=None, level=None, add_stream_handler=False):
        # type: (unittest.TestCase, Union[str, logging.Logger], str, bool) -> None
        """
        Initialize the AssertLogsContext
        :param test_case: TestCase to assert for
        :type test_case: unittest.TestCase
        :param logger_name: Logger instance or logger name to use
        :type logger_name: Union[str, logging.Logger]
        :param level: Logging level to use
        :type level: str
        :param add_stream_handler: Add an additional stream handler.
        Useful for debugging
        :type add_stream_handler: bool
        """
        _BaseTestCaseContext.__init__(self, test_case)

        self.logger_name = logger_name
        if level:
            self.level = logging._levelNames.get(level, level)
        else:
            self.level = logging.INFO
        self.msg = None
        self.add_stream_handler = add_stream_handler

    def __enter__(self):
        # type: () -> _LoggingWatcher
        """
        Enter the context manager
        This will configure the logging to use a capture handler.
        All records captured are then yielded through _LoggingWatcher
        """
        if isinstance(self.logger_name, logging.Logger):
            # Set both the local logger and the self.logger to the logger instance
            logger = self.logger = self.logger_name
        else:
            logger = self.logger = logging.getLogger(self.logger_name)
        formatter = logging.Formatter(LOG_FORMAT_UNITTEST)
        handler = _CapturingHandler()
        handler.setFormatter(formatter)

        handlers = [handler]
        if self.add_stream_handler:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            handlers.append(stream_handler)

        self.watcher = handler.watcher
        self.old_handlers = logger.handlers[:]
        self.old_level = logger.level
        self.old_propagate = logger.propagate

        logger.handlers = handlers
        logger.setLevel(self.level)
        logger.propagate = False
        return handler.watcher

    def __exit__(self, exc_type, exc_value, tb):
        # type: (any, any, any) -> bool
        """
        Exit the context manager
        Disables all the logging handlers previously added
        """
        self.logger.handlers = self.old_handlers
        self.logger.propagate = self.old_propagate
        self.logger.setLevel(self.old_level)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return False
        if len(self.watcher.records) == 0:
            self._raise_failure("No logs of level {} or higher triggered on {}".format(logging.getLevelName(self.level), self.logger.name))


class LogTestCase(unittest.TestCase):

    def assertLogs(self, logger=None, level=None, add_stream_handler=False):
        # type: (Union[str, logging.Logger], str, bool) -> _AssertLogsContext
        """
        Fail unless a log message of level *level* or higher is emitted
        on *logger_name* or its children.  If omitted, *level* defaults to
        INFO and *logger* defaults to the root logger.

        This method must be used as a context manager, and will yield
        a recording object with two attributes: `output` and `records`.
        At the end of the context manager, the `output` attribute will
        be a list of the matching formatted log messages and the
        `records` attribute will be a list of the corresponding LogRecord
        objects.

        Example::

            with self.assertLogs('foo', level='INFO') as cm:
                logging.getLogger('foo').info('first message')
                logging.getLogger('foo.bar').error('second message')
            self.assertEqual(cm.output, ['INFO:foo:first message',
                                         'ERROR:foo.bar:second message'])
        :param logger: Logger instance or logger name to use
        :type logger: Union[str, logging.Logger]
        :param level: Logging level to use
        :type level: str
        :param add_stream_handler: Add an additional stream handler.
        Useful for debugging
        :type add_stream_handler: bool
        :return: Return the assertLogsContext
        :rtype: _AssertLogsContext
        """
        return _AssertLogsContext(self, logger, level, add_stream_handler)

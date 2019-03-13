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
import gevent
from collections import deque
from contextlib import contextmanager
from gevent.coros import BoundedSemaphore
from .client import PyrakoonClient
from ovs_extensions.log.logger import Logger


class PyrakoonPool(object):
    """
    Pyrakoon pool.
    Keeps a number of Pyrakoon clients queued up to avoid waiting too long on a socket lock of a single instance
    Uses PyrakoonClient as it has retries on master loss
    """

    _logger = Logger('extensions')

    # Frequency at which the pool is populated at startup
    SPAWN_FREQUENCY = 0.1

    def __init__(self, cluster, nodes, pool_size=10, retries=10, retry_back_off_multiplier=2, retry_interval_sec=2):
        # type: (str, Dict[str, Tuple[str, int]], int, int, int, int) -> None
        """
        Initializes the client
        :param cluster: Identifier of the cluster
        :type cluster: str
        :param nodes: Dict with all node sockets. {name of the node: (ip of node, port of node)}
        :type nodes: dict
        :param pool_size: Number of clients to keep in the pool
        :type pool_size: int
        :param retries: Number of retries to do
        :type retries: int
        :param retry_back_off_multiplier: Back off multiplier. Multiplies the retry_interval_sec with this number ** retry
        :type retry_back_off_multiplier: int
        :param retry_interval_sec: Seconds to wait before retrying. Exponentially increases with every retry.
        :type retry_interval_sec: int
        """
        self.pool_size = pool_size
        self._pyrakoon_args = (cluster, nodes, retries, retry_back_off_multiplier, retry_interval_sec)
        self._sequences = {}

        self._lock = BoundedSemaphore(pool_size)

        self._clients = deque()
        for i in xrange(pool_size):
            # No clients as of yet. Decrease the count
            self._lock.acquire()
        for i in xrange(pool_size):
            gevent.spawn_later(self.SPAWN_FREQUENCY * i, self._add_client)

    def _create_new_client(self):
        # type: () -> PyrakoonClient
        """
        Create a new Arakoon client
        Using PyrakoonClient as it has retries on master loss
        :return: The created PyrakoonClient client
        :rtype: PyrakoonClient
        """
        return PyrakoonClient(*self._pyrakoon_args)

    def _add_client(self):
        # type: () -> None
        """
        Add a new client to the pool
        :return: None
        """
        sleep_time = 0.1
        while True:
            client = self._create_new_client()
            if client:
                break
            gevent.sleep(sleep_time)

        self._clients.append(client)
        self._lock.release()

    @contextmanager
    def get_client(self):
        # type: () -> Iterable[PyrakoonClient]
        """
        Get a client from the pool. Used as context manager
        """
        self._lock.acquire()
        # Client should always be present as we acquire the semaphore which would block until the are clients the in the
        # queue but checking if it's None makes the IDE not complain
        client = None
        try:
            client = self._clients.popleft()
            yield client
        # Possible catch exception that require a new client to be spawned.
        # When creating a new client, the semaphore will have to be released when spawning a new one
        # You won't be able to use the finally statement then but will have to rely on try except else
        finally:
            if client:
                self._clients.append(client)
                self._lock.release()

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


import time
import socket


class GraphiteClient(object):
    """
    Make a Graphite client, which allows data to be sent to Graphite
    """

    def __init__(self, ip, port, database):
        precursor = 'openvstorage.fwk'
        if database is not None and not database.startswith(precursor):
            precursor = '.'.join([precursor, database])
        self.precursor = precursor + '.{0} {1} {2}'   # format: precusor.env.x.y.z value timestamp

        self.ip = ip
        self.port = port

    def __str__(self):
        return 'Graphite client: path <{0}> at ({1}:{2})'.format(self.precursor, self.ip, self.port)

    def __repr__(self):
        return str(self)

    def send(self, path, data):
        # type: (str, Any) -> None
        """
        Send the statistics with client
        :param path: path in graphite to send the data to
        :param data: data to send
        :return: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            datastring = self.precursor.format(path, data, int(time.time()))  # Carbon timestamp in integers
            sock.sendto(datastring, (self.ip, self.port))
        finally:
            sock.close()

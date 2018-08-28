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
import base64
from .baseclient import BaseClient


class SimpleClient(BaseClient):
    """
    Simple API client
    - No JWT
    - No caching
    - No versioning
    - No verification
    - Authorization with username/password
    """
    def __init__(self, ip, port, credentials=None):
        """
        Initializes the object with credentials and connection information
        :param ip: IP to which to connect
        :type ip: str
        :param port: Port on which to connect
        :type port: int
        :param credentials: Credentials to connect
        :type credentials: tuple
        :return: None
        :rtype: NoneType
        """
        super(SimpleClient, self).__init__(ip, port, credentials)
        self._url = 'https://{0}:{1}/'.format(ip, port)

    def _build_headers(self):
        """
        Builds the request headers
        :return: The request headers
        :rtype: dict
        """
        headers = {'Accept': 'application/json; version={0}'.format(self._version),
                   'Content-Type': 'application/json'}
        if self._token is not None:
            headers['Authorization'] = 'Basic {0}'.format(base64.b64encode('{0}:{1}'.format(self.client_id, self.client_secret)).strip())
        return headers

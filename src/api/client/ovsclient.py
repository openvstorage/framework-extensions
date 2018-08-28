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
from .baseclient import BaseClient


class OVSClient(BaseClient):

    def wait_for_task(self, task_id, timeout=None):
        """
        Waits for a task to complete
        :param task_id: Task to wait for
        :param timeout: Time to wait for task before raising
        """
        start = time.time()
        finished = False
        previous_metadata = None
        while finished is False:
            if timeout is not None and timeout < (time.time() - start):
                raise RuntimeError('Waiting for task {0} has timed out.'.format(task_id))
            task_metadata = self.get('/tasks/{0}/'.format(task_id))
            finished = task_metadata['status'] in ('FAILURE', 'SUCCESS')
            if finished is False:
                if task_metadata != previous_metadata:
                    self._logger.debug('Waiting for task {0}, got: {1}'.format(task_id, task_metadata))
                    previous_metadata = task_metadata
                else:
                    self._logger.debug('Still waiting for task {0}...'.format(task_id))
                time.sleep(1)
            else:
                self._logger.debug('Task {0} finished, got: {1}'.format(task_id, task_metadata))
                return task_metadata['successful'], task_metadata['result']
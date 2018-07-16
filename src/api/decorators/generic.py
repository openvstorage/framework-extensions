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
API decorators
"""

from functools import wraps


class HTTPRequestGenericDecorators(object):

    @classmethod
    def wrap_data(cls, data_type='data'):
        """
        Wrap the API data in a dict with given_key

        Eg.
        def xyz: return <Data>

        @wrap_data('data_type')
        def xyz: return {'data_type': <Data>}
        """

        def wrapper(f):
            """
            Wrapper function
            """

            @wraps(f)
            def new_function(*args, **kwargs):
                """
                Return the
                """
                results = f(*args, **kwargs)
                return {data_type: results}

            return new_function

        return wrapper

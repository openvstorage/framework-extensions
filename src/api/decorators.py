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

import json
import time
import subprocess
from flask import Response
from ovs_extensions.api.exceptions import HttpBadRequestException
from ovs_extensions.dal.base import ObjectNotFoundException


class HTTPRequestDecorators(object):
    """
    Class with decorator functionality for HTTP requests
    """
    app = None
    logger = None
    version = None

    @classmethod
    def get(cls, route, authenticate=True):
        """
        GET decorator
        """
        def wrap(f):
            """
            Wrapper function
            """
            return cls.app.route(route, methods=['GET'])(cls._build_function(f, authenticate, route, 'GET'))
        return wrap

    @classmethod
    def post(cls, route, authenticate=True):
        """
        POST decorator
        """
        def wrap(f):
            """
            Wrapper function
            """
            return cls.app.route(route, methods=['POST'])(cls._build_function(f, authenticate, route, 'POST'))
        return wrap

    @classmethod
    def delete(cls, route, authenticate=True):
        """
        DELETE decorator
        """
        def wrap(f):
            """
            Wrapper function
            """
            return cls.app.route(route, methods=['DELETE'])(cls._build_function(f, authenticate, route, 'DELETE'))
        return wrap

    @classmethod
    def patch(cls, route, authenticate=True):
        """
        PATCH decorator
        """
        def wrap(f):
            """
            Wrapper function
            """
            return cls.app.route(route, methods=['PATCH'])(cls._build_function(f, authenticate, route, 'PATCH'))
        return wrap

    @classmethod
    def authorized(cls):
        """
        Indicates whether a call is authenticated
        """
        raise NotImplementedError()

    @classmethod
    def _build_function(cls, f, authenticate, route, method):
        """
        Wrapping generator
        """
        def new_function(*args, **kwargs):
            """
            Wrapped function
            """
            start = time.time()
            if authenticate is True and not cls.authorized():
                data = {'_success': False, '_error': 'Invalid credentials'}
                status_code = 401
            else:
                try:
                    if args or kwargs:
                        cls.logger.info('{0} {1} - Entering with {2} {3}'.format(method, route, json.dumps(args), json.dumps(kwargs)))
                    else:
                        cls.logger.info('{0} {1} - Entering'.format(method, route))
                    return_data = f(*args, **kwargs)
                    cls.logger.info('{0} {1} - Leaving'.format(method, route))
                    if return_data is None:
                        return_data = {}
                    if isinstance(return_data, tuple):
                        data = return_data[0]
                        status_code = return_data[1]
                    else:
                        data = return_data
                        status_code = 200
                    data['_success'] = True
                    data['_error'] = ''
                except ObjectNotFoundException as ex:
                    cls.logger.exception('DAL lookup exception')
                    data = {'_success': False, '_error': str(ex)}
                    status_code = 404
                except HttpBadRequestException as ex:
                    cls.logger.exception('API exception')
                    data = {'_success': False, '_error': str(ex)}
                    status_code = ex.status_code
                except subprocess.CalledProcessError as ex:
                    cls.logger.exception('CPE exception')
                    data = {'_success': False, '_error': ex.output if ex.output != '' else str(ex)}
                    status_code = 500
                except Exception as ex:
                    cls.logger.exception('Unexpected exception')
                    data = {'_success': False, '_error': str(ex)}
                    status_code = 500
            data['_version'] = cls.version
            data['_duration'] = time.time() - start
            return Response(json.dumps(data), content_type='application/json', status=status_code)

        new_function.original = f
        new_function.__name__ = f.__name__
        new_function.__module__ = f.__module__
        return new_function

    @classmethod
    def proper_wrap(cls, data_type):
        """
        Wrap the API data

        Eg.
        def xyz: return <Data>

        @proper_wrap('data_type')
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

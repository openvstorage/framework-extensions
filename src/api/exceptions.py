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
Custom exceptions module
"""

import json


class HttpException(RuntimeError):
    """
    Custom HTTP Exception
    """
    def __init__(self, status, error, error_description=''):
        self.status_code = status
        self.error = error
        self.error_description = error_description
        self.data = json.dumps({'error': error,
                                'error_description': error_description})


class HttpBadRequestException(HttpException):
    """
    Custom HTTP Bad Request Exception
    Thrown when an error occurs on client side
    """
    def __init__(self, error, error_description):
        super(HttpBadRequestException, self).__init__(400, error, error_description)


class HttpUnauthorizedException(HttpException):
    """
    Custom HTTP Unauthorized Exception
    Thrown when authorization is required but failed or has not been provided
    """
    def __init__(self, error, error_description):
        super(HttpUnauthorizedException, self).__init__(401, error, error_description)


class HttpForbiddenException(HttpException):
    """
    Custom HTTP Forbidden Exception
    Thrown when the request is valid, but server refuses action because eg:
      * User does not have necessary permissions for a resource
      * User needs an account of some sort
    """
    def __init__(self, error, error_description):
        super(HttpForbiddenException, self).__init__(403, error, error_description)


class HttpNotFoundException(HttpException):
    """
    Custom HTTP Not Found Exception
    Thrown when the requested resource could not be found, but may be available in the future
    """
    def __init__(self, error, error_description):
        super(HttpNotFoundException, self).__init__(404, error, error_description)


class HttpMethodNotAllowedException(HttpException):
    """
    Custom HTTP Method Not Allowed Exception
    Thrown when the request method is not supported for the requested resource, eg:
      * A GET request on a form that requires data to be presented via a POST
      * A PUT request on a read-only resource
    """
    def __init__(self, error, error_description):
        super(HttpMethodNotAllowedException, self).__init__(405, error, error_description)


class HttpNotAcceptableException(HttpException):
    """
    Custom HTTP Not Acceptable Exception
    Thrown when the requested resource is capable of generating only content not acceptable according to the Accept headers sent in the request
      * Eg: Data passed has correct format (json, str, ...), but not all required arguments have been passed 
    """
    def __init__(self, error, error_description):
        super(HttpNotAcceptableException, self).__init__(406, error, error_description)


class HttpGoneException(HttpException):
    """
    Custom HTTP Gone Exception
    Thrown when the requested resource is no longer available and will not be available again
    """
    def __init__(self, error, error_description):
        super(HttpGoneException, self).__init__(410, error, error_description)


class HttpTooManyRequestsException(HttpException):
    """
    Custom HTTP Too Many Requests Exception
    Thrown when a user has sent too many requests in a given amount of time
    """
    def __init__(self, error, error_description):
        super(HttpTooManyRequestsException, self).__init__(429, error, error_description)


class HttpInternalServerErrorException(HttpException):
    """
    Custom HTTP Internal Server Error Exception
    Thrown when an unexpected condition was encountered and no more specific message is suitable (generic server side error)
    """
    def __init__(self, error, error_description):
        super(HttpInternalServerErrorException, self).__init__(500, error, error_description)


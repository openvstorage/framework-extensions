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
import os
from functools import wraps


_files_cache = {}


class FileCache(object):
    """
    File caching object which stores both mtime and contents
    Can be used to compare too
    """
    def __init__(self, path, mtime, contents):
        # type: (str, float, Union[str, dict]) -> None
        """
        Initializes a new FileCache object
        :param path: Path to the file
        :type path: str
        :param mtime: Last modification time of the file as unix timestamp
        :type mtime: int
        :param contents: Contents of the file
        :type contents: str or dict
        """
        self.path = path
        self.mtime = mtime
        self.contents = contents

    def __eq__(self, other):
        # type: (FileCache) -> bool
        if not isinstance(other, FileCache):
            raise ValueError('The item to compare is not of type {0}'.format(FileCache))
        return self.path == other.path and self.mtime == other.mtime

    def __ne__(self, other):
        # type: (FileCache) -> bool
        return not self.__eq__(other)


def cache_file(path):
    # type: (str) -> callable
    """
    The result of the decorated function is tied to a file
    This means that the result of the function should be based of the file that is specified
    On evaluation either:
    - Returns the result of the decorated function if it runs for the first time or the file has changed
        or
    - Returns the previous result if the file did not change
    :param path: Path to the file
    :type path: str
    """
    def is_cache_valid():
        # type: () -> bool
        """
        Determine if the cache is still valid
        :return: True if the cache is still valid else false
        :rtype: bool
        """
        if path in _files_cache:
            file_cache = _files_cache[path]  # type: FileCache
            return file_cache.mtime == os.path.getmtime(path)
        return False

    def decorator(f):
        # type: (callable) -> callable
        @wraps(f)
        def return_cache(*args, **kwargs):
            # type: (*any, **any) -> any
            if path in _files_cache and is_cache_valid():
                file_cache = _files_cache[path]  # type: FileCache
                return file_cache.contents
            else:
                # Store to cache
                contents = f(*args, **kwargs)
                file_cache = FileCache(path, os.path.getmtime(path), contents)
                _files_cache[path] = file_cache
                return file_cache.contents
        return return_cache
    return decorator

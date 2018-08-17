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
    """
    Caches a files contents
    Used on methods which read a file. Inspects the modified time with the one cached to determine
    if the file changed
    :param path: Path to the file
    :return: The contents of the file (if any)
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
        @wraps(f)
        def return_cache(*args, **kwargs):
            if path in _files_cache and is_cache_valid():
                print 'from_cache'
                file_cache = _files_cache[path]  # type: FileCache
                return file_cache.contents
            else:
                # Store to cache
                file_cache = FileCache(path, os.path.getmtime(path), f(*args, **kwargs))
                _files_cache[path] = file_cache
                return file_cache.contents
        return return_cache
    return decorator

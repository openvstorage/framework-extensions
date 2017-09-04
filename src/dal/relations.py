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
RelationMapper module
"""
import os
import imp
import inspect


class RelationMapper(object):
    """
    The RelationMapper is responsible for loading the relational structure
    of the objects.
    """

    cache = {}

    @staticmethod
    def load_foreign_relations(object_type):
        """
        This method will return a mapping of all relations towards a certain object type.
        The resulting mapping will be cached in-process
        """
        relation_key = '{0}_relations_{1}'.format(object_type.NAME, object_type.__name__.lower())
        if relation_key in RelationMapper.cache:
            return RelationMapper.cache[relation_key]
        relation_info = {}
        path = '/'.join([object_type.SOURCE_FOLDER, 'dal', 'objects'])
        for filename in os.listdir(path):
            if os.path.isfile('/'.join([path, filename])) and filename.endswith('.py'):
                name = filename.replace('.py', '')
                mod = imp.load_source(name, '/'.join([path, filename]))
                for member in inspect.getmembers(mod):
                    if inspect.isclass(member[1]) and member[1].__module__ == name:
                        current_class = member[1]
                        if 'Base' not in current_class.__name__:
                            object_class = None
                            # __mro__ for dal.base.Base extended classes should look something like this:
                            #     [ <class 'setting.Setting'>,
                            #       <class 'source.dal.asdbase.ASDBase'>,
                            #       <class 'ovs_extensions.dal.base.Base'>,
                            #       < type 'object'> ]
                            for this_class in current_class.__mro__:
                                if 'Base' in this_class.__name__:
                                    object_class = this_class
                                    break
                            if object_class is not None:
                                # noinspection PyProtectedMember
                                for relation in object_class._relations:
                                    if relation[1] is None:
                                        remote_class = object_class
                                    else:
                                        remote_class = relation[1]
                                    if remote_class.__name__ == object_type.__name__:
                                        relation_info[relation[2]] = {'class': object_class,
                                                                      'key': relation[0]}
        RelationMapper.cache[relation_key] = relation_info
        return relation_info

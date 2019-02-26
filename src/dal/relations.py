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
import inspect
from ovs_extensions.generic.plugin import PluginController

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
        for current_class in PluginController.get_dal_objects():
            object_class = current_class
            # __mro__ for dal.base.Base extended classes should look something like this:
            #     [ <class 'setting.Setting'>,
            #       <class 'source.dal.asdbase.ASDBase'>,
            #       <class 'ovs_extensions.dal.base.Base'>,
            #       < type 'object'> ]
            for relation in object_class._relations:
                if relation[1] is None:  #todo check objects of relation tuple
                    remote_class = object_class
                else:
                    remote_class = relation[1]
                if remote_class.__name__ == object_type.__name__:
                    relation_info[relation[2]] = {'class': object_class,
                                                  'key': relation[0]}
        RelationMapper.cache[relation_key] = relation_info
        return relation_info

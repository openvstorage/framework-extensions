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
This package contains the DAL object's base class.
"""

import json
import sqlite3
from ovs_extensions.dal.relations import RelationMapper
from ovs_extensions.generic.filemutex import file_mutex


class ObjectNotFoundException(Exception):
    """ Exception indicating that an object in the DAL was not found. """
    pass


# noinspection SqlDialectInspection,SqlNoDataSourceInspection,PyTypeChecker,PyProtectedMember
class Base(object):
    """
    Base object that is inherited by all DAL objects. It contains base logic like save, delete, ...
    """
    NAME = None
    SOURCE_FOLDER = None
    DATABASE_FOLDER = None

    _table = None
    _dynamics = []
    _relations = []
    _properties = []

    def __init__(self, identifier=None, locked=True):
        """
        Initializes a new object. If no identifier is passed in, a new one is created.
        :param identifier: Optional identifier (primary key)
        :type identifier: int
        :param locked: Indicates whether the constructor should lock the DB
        :type locked: bool
        """
        self.id = identifier
        try:
            if locked is True:
                self.lock().acquire()
            self._ensure_table()
            with self.connector() as connection:
                if identifier is not None:
                    cursor = connection.cursor()
                    cursor.execute('SELECT * FROM {0} WHERE id=?'.format(self._table), [self.id])
                    row = cursor.fetchone()
                    if row is None:
                        raise ObjectNotFoundException()
                    for prop in self._properties:
                        setattr(self, prop.name, Base._deserialize(prop.property_type, row[prop.name]))
                    for relation in self._relations:
                        setattr(self, '_{0}'.format(relation[0]), {'id': row['_{0}_id'.format(relation[0])],
                                                                   'object': None})
                else:
                    for prop in self._properties:
                        setattr(self, prop.name, None)
                    for relation in self._relations:
                        setattr(self, '_{0}'.format(relation[0]), {'id': None,
                                                                   'object': None})
        finally:
            self.lock().release()
        for relation in self._relations:
            self._add_relation(relation)
        for key, relation_info in RelationMapper.load_foreign_relations(self.__class__).iteritems():
            self._add_foreign_relation(key, relation_info)
        for key in self._dynamics:
            self._add_dynamic(key)

    @classmethod
    def connector(cls):
        """ Creates and returns a new connection to SQLite. """
        connection = sqlite3.connect('{0}/main.db'.format(cls.DATABASE_FOLDER))
        connection.row_factory = sqlite3.Row
        return connection

    @classmethod
    def lock(cls):
        """ Returns a file lock context manager """
        return file_mutex('{0}/main.lock'.format(cls.DATABASE_FOLDER))

    def _add_dynamic(self, key):
        """ Generates a new dynamic value on an object. """
        setattr(self.__class__, key, property(lambda s: getattr(s, '_{0}'.format(key))()))

    def _add_foreign_relation(self, key, relation_info):
        """ Generates a new foreign relation on an object. """
        setattr(self.__class__, key, property(lambda s: s._get_foreign_relation(relation_info)))

    def _get_foreign_relation(self, relation_info):
        """ Getter logic for a foreign relation. """
        remote_class = relation_info['class']
        remote_class._ensure_table()
        entries = []
        with self.connector() as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT id FROM {0} WHERE _{1}_id=?'.format(remote_class._table, relation_info['key']),
                           [self.id])
            for row in cursor.fetchall():
                entries.append(remote_class(row['id']))
        return entries

    def _add_relation(self, relation):
        """ Generates a new relation on an object. """
        setattr(self.__class__, relation[0], property(lambda s: s._get_relation(relation),
                                                      lambda s, v: s._set_relation(relation, v)))
        setattr(self.__class__, '{0}_id'.format(relation[0]), property(lambda s: s._get_relation_id(relation)))

    def _get_relation(self, relation):
        """ Getter for a relation. """
        data = getattr(self, '_{0}'.format(relation[0]))
        if data['object'] is None and data['id'] is not None:
            data['object'] = relation[1](data['id'])
        return data['object']

    def _set_relation(self, relation, value):
        """ Setter for a relation. """
        data = getattr(self, '_{0}'.format(relation[0]))
        if value is None:
            data['id'] = None
            data['object'] = None
        else:
            data['id'] = value.id
            data['object'] = value

    def _get_relation_id(self, relation):
        """ Getter for a relation identifier. """
        return getattr(self, '_{0}'.format(relation[0]))['id']

    def save(self):
        """
        Saves the current object. If not existing, it is created and the identifier field is filled.
        :return: None
        """
        prop_values = []
        for prop in self._properties:
            if prop.property_type is None and prop.mandatory is True and getattr(self, prop.name) is None:  # None value would otherwise be JSON serialized to 'null', bypassing the mandatory CONSTRAINT
                prop_values.append(None)
            else:
                prop_values.append(Base._serialize(prop.property_type, getattr(self, prop.name)))
        prop_values.extend([getattr(self, '_{0}'.format(relation[0])).get('id') for relation in self._relations])
        if self.id is None:
            field_names = ', '.join([prop.name for prop in self._properties] +
                                    ['_{0}_id'.format(relation[0]) for relation in self._relations])
            prop_statement = ', '.join('?' for _ in self._properties + self._relations)
            with self.lock(), self.connector() as connection:
                cursor = connection.cursor()
                cursor.execute('INSERT INTO {0}({1}) VALUES ({2})'.format(self._table, field_names, prop_statement),
                               prop_values)
                self.id = cursor.lastrowid
        else:
            prop_statement = ', '.join(['{0}=?'.format(prop.name) for prop in self._properties] +
                                       ['_{0}_id=?'.format(relation[0]) for relation in self._relations])
            with self.lock(), self.connector() as connection:
                connection.execute('UPDATE {0} SET {1} WHERE id=? LIMIT 1'.format(self._table, prop_statement),
                                   prop_values + [self.id])

    def delete(self):
        """
        Deletes the current object from the SQLite database.
        :return: None
        """
        with self.lock(), self.connector() as connection:
            connection.execute('DELETE FROM {0} WHERE id=? LIMIT 1'.format(self._table), [self.id])

    @staticmethod
    def _get_prop_type(prop_type):
        """ Translates a python type to a SQLite type. """
        if prop_type in [int, bool]:
            return 'INTEGER'
        if prop_type in [str, basestring, unicode, list, dict, None]:
            return 'TEXT'
        raise ValueError('The type {0} is not supported. Supported types: int, str, list, dict, bool'.format(prop_type))

    @staticmethod
    def _deserialize(prop_type, data):
        """ De-serializes a SQLite field to a python type. """
        if prop_type in [int, str, basestring, unicode]:
            return data
        if prop_type in [list, dict, None]:
            return json.loads(data) if data is not None else None
        if prop_type in [bool]:
            return data == 1
        raise ValueError('The type {0} is not supported. Supported types: int, str, list, dict, bool'.format(prop_type))

    @staticmethod
    def _serialize(prop_type, data):
        """ Serializes a python type to a SQLite field. """
        if prop_type in [int, str, basestring, unicode]:
            return data
        if prop_type in [list, dict, None]:
            return json.dumps(data, sort_keys=True)
        if prop_type in [bool]:
            return 1 if data else 0
        raise ValueError('The type {0} is not supported. Supported types: int, str, list, dict, bool'.format(prop_type))

    @classmethod
    def _ensure_table(cls):
        relation_list = ['_{0}_id'.format(relation[0]) for relation in cls._relations]
        relations = ['{0} INTEGER'.format(relation) for relation in relation_list]
        properties = ['{0} {1} {2} {3}'.format(prop.name,
                                               Base._get_prop_type(prop.property_type),
                                               'NOT NULL' if prop.mandatory is True else '',
                                               'UNIQUE' if prop.unique is True else '') for prop in cls._properties]
        primary_key = ['id INTEGER PRIMARY KEY AUTOINCREMENT']

        with cls.connector() as connection:
            connection.execute('CREATE TABLE IF NOT EXISTS {0} ({1})'.format(cls._table, ', '.join(primary_key + properties + relations)))
            cursor = connection.cursor()
            cursor.execute('PRAGMA table_info({0})'.format(cls._table))
            current_relations = []
            current_properties = []
            for row in cursor.fetchall():
                if row['name'].startswith('_'):
                    current_relations.append(row['name'])
                else:
                    current_properties.append(row['name'])

            # ALTER TABLE does not allow to add columns with UNIQUE or NOT NULL constraints
            for prop in cls._properties:
                if prop.name not in current_properties:
                    connection.execute('ALTER TABLE {0} ADD COLUMN {1} {2}'.format(cls._table,
                                                                                   prop.name,
                                                                                   Base._get_prop_type(prop.property_type)))

            for rel_name in relation_list:
                if rel_name not in current_relations:
                    connection.execute('ALTER TABLE {0} ADD COLUMN {1} INTEGER'.format(cls._table, rel_name))

    @classmethod
    def _update_table(cls):
        # Modifies the current table settings according to the object definition
        # use with caution (only during code migrations)
        # ALTER TABLE does not allow to add columns with UNIQUE or NOT NULL constraints
        """
        Change properties of table
        Allowed args:
        :param property_name: Property name
        :type property_name: str
        :param mandatory: whether or not the property is mandatory
        :type mandatory: bool
        :param not_null: whether or not the property can be null
        :type not_null: bool
        :param unique: whether or not the property must be unique
        :type unique: bool
        :return:
        """
        new_property = ['{0} {1} {2} {3}'.format(prop.name,
                                               Base._get_prop_type(prop.property_type),
                                               'NOT NULL' if prop.mandatory is True else '',
                                               'UNIQUE' if prop.unique is True else '') for prop in cls._properties]
        print new_property
        with cls.connector() as con:
            cur = con.cursor()
            cur.execute("select * from sqlite_master ")

            schema = cur.fetchone()
            con.close()
            print schema
            entries = [tmp.strip() for tmp in schema[0].splitlines() if tmp.find("constraint") >= 0 or tmp.find("unique") >= 0]
            for i in entries: print(i)

        # PRAGMA foreign_keys = off;
        # BEGIN TRANSACTION;
        # ALTER TABLE tablename RENAME TO _old_table

        # CREATE TABLE tablename (
        #    ( column 1 datatype [)
    def __repr__(self):
        """ Short representation of the object. """
        return '<{0} (id: {1}, at: {2})>'.format(self.__class__.__name__, self.id, hex(id(self)))

    def export(self):
        """ Exports the object """
        data = {'id': self.id}
        for prop in self._properties:
            data[prop.name] = getattr(self, prop.name)
        for relation in self._relations:
            name = '{0}_id'.format(relation[0])
            data[name] = getattr(self, name)
        for dynamic in self._dynamics:
            data[dynamic] = getattr(self, dynamic)
        return data

    def __str__(self):
        """ Returns a full representation of the object. """
        return json.dumps(self.export(), indent=4, sort_keys=True)

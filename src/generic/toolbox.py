# Copyright (C) 2016 iNuron NV
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
ExtensionsToolbox module
"""
import re
import sys
import copy


class ExtensionsToolbox(object):
    """
    Generic class for various methods
    """
    regex_ip = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))$')
    regex_guid = re.compile('^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$')
    regex_vpool = re.compile('^[0-9a-z][\-a-z0-9]{1,20}[a-z0-9]$')
    regex_preset = re.compile('^[0-9a-zA-Z][a-zA-Z0-9-_]{1,18}[a-zA-Z0-9]$')
    regex_backend = re.compile('^[0-9a-z][\-a-z0-9]{1,48}[a-z0-9]$')
    regex_ip_port = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)):[0-9]{4,5}$')
    regex_ip_subnet = re.compile('^(((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(/(8|9|(1|2)[0-9]|30))?$')
    compiled_regex_type = type(re.compile('some_regex'))

    @staticmethod
    def remove_prefix(string, prefix):
        """
        Removes a prefix from the beginning of a string
        :param string: The string to clean
        :param prefix: The prefix to remove
        :return: The cleaned string
        :rtype: str
        """
        if string.startswith(prefix):
            return string[len(prefix):]
        return string

    @staticmethod
    def edit_version_file(client, package_name, old_run_file, new_run_file=None):
        """
        Edit a run version file in order to mark it for 'reboot' or 'removal'
        :param client: Client on which to edit the version file
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :param package_name: Name of the package to check on in the version file
        :type package_name: str
        :param old_run_file: Filesystem location of the service version file which needs to be edited
        :type old_run_file: str
        :param new_run_file: Filesystem location of the service version file which needs to be written. When specified the old service file will be marked for removal
        :type new_run_file: str
        :return: None
        :rtype: NoneType
        """
        if client.file_exists(filename=old_run_file):
            contents = client.file_read(old_run_file).strip()
            if new_run_file is not None:  # Scenario in which we will mark the old version file for 'removal' and the new version file for 'reboot'
                client.run(['mv', old_run_file, '{0}.remove'.format(old_run_file)])
            else:  # Scenario in which we will mark the old version file for 'reboot'
                new_run_file = old_run_file

            if '-reboot' not in contents:
                if '=' in contents:
                    contents = ';'.join(['{0}-reboot'.format(part) for part in contents.split(';') if package_name in part])
                else:
                    contents = '{0}-reboot'.format(contents)
                client.file_write(filename=new_run_file, contents=contents)
                client.file_chown(filenames=[new_run_file], user='ovs', group='ovs')

    @staticmethod
    def check_type(value, required_type):
        """
        Validates whether a certain value is of a given type. Some types are treated as special
        case:
          - A 'str' type accepts 'str', 'unicode' and 'basestring'
          - A 'float' type accepts 'float', 'int'
          - A list instance acts like an enum
        :param value: Value to check
        :type value: object
        :param required_type: Expected type for value
        :type required_type: any
        :return: None
        :rtype: NoneType
        """
        given_type = type(value)
        if required_type is str:
            correct = isinstance(value, basestring)
            allowed_types = ['str', 'unicode', 'basestring']
        elif required_type is float:
            correct = isinstance(value, float) or isinstance(value, int)
            allowed_types = ['float', 'int']
        elif required_type is int:
            correct = isinstance(value, int) or isinstance(value, long)
            allowed_types = ['int', 'long']
        elif isinstance(required_type, list):
            # We're in an enum scenario. Field_type isn't a real type, but a list containing
            # all possible enum values. Here as well, we need to do some str/unicode/basestring
            # checking.
            if isinstance(required_type[0], basestring):
                value = str(value)
            correct = value in required_type
            allowed_types = required_type
            given_type = value
        else:
            correct = isinstance(value, required_type)
            allowed_types = [required_type.__name__]

        return correct, allowed_types, given_type

    @staticmethod
    def verify_required_params(required_params, actual_params, verify_keys=False, return_errors=False):
        """
        Verify whether the actual parameters match the required parameters
        :param required_params: Required parameters which actual parameters have to meet
        :type required_params: dict
        :param actual_params: Actual parameters to check for validity
        :type actual_params: dict
        :param verify_keys: Verify whether the passed in keys are actually part of the required keys
        :type verify_keys: bool
        :param return_errors: Return the list of errors instead of raising (Defaults to False)
        :type return_errors: bool
        :return: None or list of errors
        :rtype: NoneType or list
        """
        if not isinstance(required_params, dict) or not isinstance(actual_params, dict):
            raise RuntimeError('Required and actual parameters must be of type dictionary')

        error_messages = []
        if verify_keys is True:
            for key in actual_params:
                if key not in required_params:
                    error_messages.append('Specified parameter "{0}" is not valid'.format(key))

        for required_key, key_info in required_params.iteritems():
            expected_type = key_info[0]
            expected_value = key_info[1]
            optional = len(key_info) == 3 and key_info[2] is False

            if optional is True and (required_key not in actual_params or actual_params[required_key] in ('', None)):
                continue

            if required_key not in actual_params:
                error_messages.append('Missing required param "{0}" in actual parameters'.format(required_key))
                continue

            mandatory_or_optional = 'Optional' if optional is True else 'Mandatory'
            actual_value = actual_params[required_key]
            if ExtensionsToolbox.check_type(actual_value, expected_type)[0] is False:
                error_messages.append('{0} param "{1}" is of type "{2}" but we expected type "{3}"'.format(mandatory_or_optional, required_key, type(actual_value), expected_type))
                continue

            if expected_value is None:
                continue

            if expected_type == list:
                if type(expected_value) == ExtensionsToolbox.compiled_regex_type:  # List of strings which need to match regex
                    for item in actual_value:
                        if not re.match(expected_value, item):
                            error_messages.append('{0} param "{1}" has an item "{2}" which does not match regex "{3}"'.format(mandatory_or_optional, required_key, item, expected_value.pattern))
            elif expected_type == dict:
                ExtensionsToolbox.verify_required_params(expected_value, actual_params[required_key])
            elif expected_type == int or expected_type == float:
                if isinstance(expected_value, list) and actual_value not in expected_value:
                    error_messages.append('{0} param "{1}" with value "{2}" should be 1 of the following: {3}'.format(mandatory_or_optional, required_key, actual_value, expected_value))
                if isinstance(expected_value, dict):
                    minimum = expected_value.get('min', sys.maxint * -1)
                    maximum = expected_value.get('max', sys.maxint)
                    if not minimum <= actual_value <= maximum:
                        error_messages.append('{0} param "{1}" with value "{2}" should be in range: {3} - {4}'.format(mandatory_or_optional, required_key, actual_value, minimum, maximum))
                    if actual_value in expected_value.get('exclude', []):
                        error_messages.append('{0} param "{1}" cannot have value {2}'.format(mandatory_or_optional, required_key, actual_value))
            else:
                if ExtensionsToolbox.check_type(expected_value, list)[0] is True and actual_value not in expected_value:
                    error_messages.append('{0} param "{1}" with value "{2}" should be 1 of the following: {3}'.format(mandatory_or_optional, required_key, actual_value, expected_value))
                elif ExtensionsToolbox.check_type(expected_value, ExtensionsToolbox.compiled_regex_type)[0] is True and not re.match(expected_value, actual_value):
                    error_messages.append('{0} param "{1}" with value "{2}" does not match regex "{3}"'.format(mandatory_or_optional, required_key, actual_value, expected_value.pattern))
        if len(error_messages) > 0 and return_errors is False:
            raise RuntimeError('Invalid parameters detected\n' + '\n'.join(error_messages))
        return error_messages

    @staticmethod
    def advanced_sort(element, separator):
        """
        Function which can be used to sort names
        Eg: Sorting service_1, service_2, service_10
            will result in service_1, service_2, service_10
            io service_1, service_10, service_2
        :param element: Element to sort
        :type element: str
        :param separator: Separator to split the element on
        :type separator: str
        :return: Element split on separator and digits converted to floats
        :rtype: Tuple
        """
        entries = element.split(separator)
        for index in xrange(len(entries)):
            try:
                entries[index] = float(entries[index])
            except ValueError:
                pass
        return tuple(entries)

    @staticmethod
    def convert_to_days_hours_minutes_seconds(seconds):
        """
        Convert the specified amount of seconds to w days, x hours, y minutes and z seconds
        :param seconds: Seconds to convert
        :type seconds: int
        """
        if not isinstance(seconds, int):
            raise ValueError('Seconds must be of type "int"')

        days = seconds / 86400  # 24 * 60 * 60
        rest1 = seconds % 86400
        hours = rest1 / 3600  # 60 * 60
        rest2 = rest1 % 3600
        minutes = rest2 / 60
        seconds = rest2 % 60
        return days, hours, minutes, seconds

    @staticmethod
    def convert_byte_size_to_human_readable(size, multiplier=1024, decimals=0):
        """
        Convert the specified size (in bytes) to human readable format
        :param size: Size to be converted
        :type size: int
        :param multiplier: Multiplier to use. Either 1000 or 1024
        :type multiplier: int
        :param decimals: Amount of decimals returned
        :type decimals: int
        :return: Human readable form of the size
        :rtype: str
        """
        ExtensionsToolbox.verify_required_params(actual_params={'size': size,
                                                                'decimals': decimals,
                                                                'multiplier': multiplier},
                                                 required_params={'size': (int, None),
                                                                  'decimals': (int, range(6)),
                                                                  'multiplier': (int, [1000, 1024])})

        units = {1000: ['B', 'KB', 'MB', 'GB', 'TB'],
                 1024: ['B', 'KiB', 'MiB', 'GiB', 'TiB']}[multiplier]

        counter = 0
        negative = size < 0
        size = abs(size)
        while size >= multiplier and counter < 4:
            size /= float(multiplier)
            counter += 1

        size = size * -1 if negative is True else size * 1
        return '{{0:.{0}f}}{{1}}'.format(decimals).format(size, units[counter])

    @staticmethod
    def merge_dicts(dict1, dict2):
        """
        Merge dict2 in dict1
        :param dict1: Dictionary to start from
        :type dict1: dict
        :param dict2: Dictionary to merge into dict1
        :type dict2: dict
        :return: Merged dictionary
        :rtype: dict
        """
        if not isinstance(dict1, dict):
            raise ValueError('Dict1 should be of type dict')
        if not isinstance(dict2, dict):
            raise ValueError('Dict2 should be of type dict')

        copy_dict1 = copy.deepcopy(dict1)
        for key, value in dict1.iteritems():
            if key not in dict2:
                # No need to copy anything from dict2 into dict1, since key not present
                continue

            if isinstance(value, dict) and isinstance(dict2[key], dict):
                copy_dict1[key] = copy.deepcopy(ExtensionsToolbox.merge_dicts(dict1=value, dict2=dict2[key]))
            else:
                if isinstance(value, list) and isinstance(dict2[key], list):
                    copy_dict1[key] = value + dict2[key]
                elif isinstance(value, tuple) and isinstance(dict2[key], tuple):
                    copy_dict1[key] = value + dict2[key]
                elif isinstance(value, set) and isinstance(dict2[key], set):
                    value.update(dict2[key])
                    copy_dict1[key] = value

        # Add items present in dict2 to dict1
        for key, value in dict2.iteritems():
            if key not in copy_dict1:
                copy_dict1[key] = value
        return copy_dict1

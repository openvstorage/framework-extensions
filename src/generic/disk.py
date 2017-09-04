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
Disk module
"""

import re
import uuid
from subprocess import check_output, CalledProcessError
from ovs_extensions.generic.filemutex import file_mutex


class DiskTools(object):
    """
    This class contains various helper methods wrt Disk maintenance
    """
    logger = None  # Instantiated by classes inheriting from this 1

    def __init__(self):
        raise Exception('Cannot instantiate, completely static class')

    @classmethod
    def create_partition(cls, disk_alias, disk_size, partition_start, partition_size):
        """
        Creates a partition
        :param disk_alias: Path of the disk device
        :type disk_alias: str
        :param disk_size: Total size of disk
        :type disk_size: int
        :param partition_start: Start of partition in bytes
        :type partition_start: int
        :param partition_size: Size of partition in bytes
        :type partition_size: int
        :return: None
        """
        # Verify current label type and add GPT label if none present
        disk_alias = disk_alias.replace(r"'", r"'\''")
        try:
            command = "parted '{0}' print | grep 'Partition Table'".format(disk_alias)
            cls.logger.info('Checking partition label-type with command: {0}'.format(command))
            label_type = check_output(command, shell=True).strip().split(': ')[1]
        except CalledProcessError:
            label_type = 'error'
        if label_type in ('error', 'unknown'):
            try:
                cls.logger.info('Adding GPT label and trying to create partition again')
                check_output("parted '{0}' -s mklabel gpt".format(disk_alias), shell=True)
                label_type = 'gpt'
            except Exception:
                cls.logger.error('Error during label creation')
                raise

        # Determine command to use based upon label type
        start = int(round(float(partition_start) / disk_size * 100))
        end = int(round(float(partition_size) / disk_size * 100)) + start
        if end > 100:
            end = 100

        if label_type == 'gpt':
            command = "parted '{0}' -a optimal -s mkpart '{1}' '{2}%' '{3}%'".format(disk_alias, uuid.uuid4(), start, end)
        elif label_type == 'msdos':
            command = "parted '{0}' -a optimal -s mkpart primary ext4 '{1}%' '{2}%'".format(disk_alias, start, end)
        elif label_type == 'bsd':
            command = "parted '{0}' -a optimal -s mkpart ext4 '{1}%' '{2}%'".format(disk_alias, start, end)
        else:
            raise ValueError('Unsupported label-type detected: {0}'.format(label_type))

        # Create partition
        cls.logger.info('Label type detected: {0}'.format(label_type))
        cls.logger.info('Command to create partition: {0}'.format(command))
        check_output(command, shell=True)

    @classmethod
    def make_fs(cls, partition_alias):
        """
        Creates a filesystem
        :param partition_alias: Path of the partition
        :type partition_alias: str
        :return: None
        """
        try:
            check_output("mkfs.ext4 -q '{0}'".format(partition_alias.replace(r"'", r"'\''")), shell=True)
        except Exception:
            cls.logger.error('Error during filesystem creation')
            raise

    @classmethod
    def add_fstab(cls, partition_aliases, mountpoint, filesystem):
        """
        Add entry to /etc/fstab for mountpoint
        :param partition_aliases: Possible aliases of the partition to add
        :type partition_aliases: list
        :param mountpoint: Mountpoint on which device is mounted
        :type mountpoint: str
        :param filesystem: Filesystem used
        :type filesystem: str
        :return: None
        """
        if len(partition_aliases) == 0:
            raise ValueError('No partition aliases provided')

        with open('/etc/fstab', 'r') as fstab_file:
            lines = [line.strip() for line in fstab_file.readlines()]

        osmanager = cls._get_os_manager()
        used_path = None
        used_index = None
        mount_line = None
        for device_alias in partition_aliases:
            for index, line in enumerate(lines):
                if line.startswith('#'):
                    continue
                if line.startswith(device_alias) and re.match('^{0}\s+'.format(re.escape(device_alias)), line):
                    used_path = device_alias
                    used_index = index
                if len(line.split()) == 6 and line.split()[1] == mountpoint:  # Example line: 'UUID=40d99523-a1e7-4374-84f2-85b5d14b516e  /  swap  sw  0  0'
                    mount_line = line
            if used_path is not None:
                break

        if used_path is None:  # Partition not yet present with any of its possible aliases
            lines.append(osmanager.get_fstab_entry(partition_aliases[0], mountpoint, filesystem))
        else:  # Partition present, update information
            lines.pop(used_index)
            lines.insert(used_index, osmanager.get_fstab_entry(used_path, mountpoint, filesystem))

        if mount_line is not None:  # Mount point already in use by another device (potentially same device, but other device_path)
            lines.remove(mount_line)

        with file_mutex('ovs-fstab-lock'):
            with open('/etc/fstab', 'w') as fstab_file:
                fstab_file.write('{0}\n'.format('\n'.join(lines)))

    @staticmethod
    def mountpoint_exists(mountpoint):
        """
        Verify whether a mount point exists by browsing /etc/fstab
        :param mountpoint: Mount point to check
        :type mountpoint: str
        :return: True if mount point exists, False otherwise
        :rtype: bool
        """
        with open('/etc/fstab', 'r') as fstab_file:
            for line in fstab_file.readlines():
                if re.search('\s+{0}\s+'.format(re.escape(mountpoint)), line):
                    return True
        return False

    @classmethod
    def mount(cls, mountpoint):
        """
        Mount a partition
        :param mountpoint: Mount point on which to mount the partition
        :type mountpoint: str
        :return: None
        """
        try:
            mountpoint = mountpoint.replace(r"'", r"'\''")
            check_output("mkdir -p '{0}'".format(mountpoint), shell=True)
            check_output("mount '{0}'".format(mountpoint), shell=True)
        except Exception:
            cls.logger.exception('Error during mount')
            raise

    @classmethod
    def umount(cls, mountpoint):
        """
        Unmount a partition
        :param mountpoint: Mount point to un-mount
        :type mountpoint: str
        :return: None
        """
        try:
            check_output("umount '{0}'".format(mountpoint.replace(r"'", r"'\''")), shell=True)
        except Exception:
            cls.logger.exception('Unable to umount mount point {0}'.format(mountpoint))

    @classmethod
    def _get_os_manager(cls):
        raise NotImplementedError()

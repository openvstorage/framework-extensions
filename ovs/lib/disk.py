# Copyright 2015 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DiskController module
"""
import re
import os
import time
from pyudev import Context
from celery.schedules import crontab
from ovs.celery_run import celery
from ovs.log.logHandler import LogHandler
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.remote import Remote
from ovs.lib.helpers.decorators import ensure_single

logger = LogHandler('lib', name='disk')


class DiskController(object):
    """
    Contains all BLL wrt physical Disks
    """

    @staticmethod
    @celery.task(name='ovs.disk.sync_with_reality', bind=True, schedule=crontab(minute='*/5'))
    @ensure_single(['ovs.disk.sync_with_reality'])
    def sync_with_reality(storagerouter_guid=None):
        """
        Syncs the Disks from all StorageRouters with the reality.
        """
        storagerouters = []
        if storagerouter_guid is not None:
            storagerouters.append(StorageRouter(storagerouter_guid))
        else:
            storagerouters = StorageRouterList.get_storagerouters()
        for storagerouter in storagerouters:
            client = SSHClient(storagerouter.ip, username='root')
            configuration = {}
            # Gather mount data
            mount_mapping = {}
            mount_data = client.run('mount')
            for mount in mount_data.splitlines():
                mount = mount.strip()
                match = re.search('/dev/(.+?) on (/.*?) type.*', mount)
                if match is not None:
                    mount_mapping[match.groups()[0]] = match.groups()[1]
            # Gather disk information
            with Remote(storagerouter.ip, [Context]) as remote:
                context = remote.Context()
                devices = [device for device in context.list_devices(subsystem='block')
                           if 'ID_TYPE' in device and device['ID_TYPE'] == 'disk']
                for device in devices:
                    is_partition = device['DEVTYPE'] == 'partition'
                    device_path = device['DEVNAME']
                    device_name = device_path.split('/')[-1]
                    partition_id = None
                    partition_name = None
                    if is_partition is True:
                        partition_id = device['ID_PART_ENTRY_NUMBER']
                        partition_name = device_name
                        device_name = device_name[:0 - int(len(partition_id))]
                    if device_name not in configuration:
                        configuration[device_name] = {'partitions': {}}
                    path = None
                    for path_type in ['by-id', 'by-uuid']:
                        if path is not None:
                            break
                        for item in device['DEVLINKS'].split(' '):
                            if path_type in item:
                                path = item
                    if path is None:
                        path = device_path
                    sectors = int(client.run('cat /sys/block/{0}/size'.format(device_name)))
                    sector_size = int(client.run('cat /sys/block/{0}/queue/hw_sector_size'.format(device_name)))
                    rotational = int(client.run('cat /sys/block/{0}/queue/rotational'.format(device_name)))
                    if is_partition is True:
                        if device['ID_PART_ENTRY_TYPE'] == '0x5':
                            continue  # This is an extended partition, let's skip that one
                        configuration[device_name]['partitions'][partition_id] = {'offset': int(device['ID_PART_ENTRY_OFFSET']) * sector_size,
                                                                                  'size': int(device['ID_PART_ENTRY_SIZE']) * sector_size,
                                                                                  'path': path,
                                                                                  'state': 'OK'}
                        partition_data = configuration[device_name]['partitions'][partition_id]
                        if partition_name in mount_mapping:
                            mountpoint = mount_mapping[partition_name]
                            partition_data['mountpoint'] = mountpoint
                            partition_data['inode'] = os.stat(mountpoint).st_dev
                            del mount_mapping[partition_name]
                            try:
                                client.run('touch {0}/{1}; rm {0}/{1}'.format(mountpoint, str(time.time())))
                            except:
                                partition_data['state'] = 'ERROR'
                                pass
                        if 'ID_FS_TYPE' in device:
                            partition_data['filesystem'] = device['ID_FS_TYPE']
                    else:
                        configuration[device_name].update({'name': device_name,
                                                           'path': path,
                                                           'vendor': device['ID_VENDOR'] if 'ID_VENDOR' in device else None,
                                                           'model': device['ID_MODEL'] if 'ID_MODEL' in device else None,
                                                           'size': sector_size * sectors,
                                                           'is_ssd': rotational == 0,
                                                           'state': 'OK'})
                    for partition_name in mount_mapping:
                        device_name = partition_name.split('/')[-1]
                        match = re.search('^(.+?)(\d+)$', device_name)
                        if match is not None:
                            device_name = match.groups()[0]
                            partition_id = match.groups()[1]
                        if device_name not in configuration:
                            configuration[device_name] = {'partitions': {},
                                                          'state': 'MISSING'}
                        configuration[device_name]['partitions'][partition_id] = {'mountpoint': mount_mapping[partition_name],
                                                                                  'state': 'MISSING'}
            # Sync the model
            disk_names = []
            for disk in storagerouter.disks:
                if disk.name not in configuration:
                    for partition in disk.partitions:
                        partition.delete()
                    disk.delete()
                else:
                    disk_names.append(disk.name)
                    DiskController._update_disk(disk, configuration[disk.name])
                    partitions = []
                    partition_info = configuration[disk.name]['partitions']
                    for partition in disk.partitions:
                        if partition.id not in partition_info:
                            partition.delete()
                        else:
                            partitions.append(partition.id)
                            DiskController._update_partition(partition, partition_info[partition.id])
                    for partition_id in partition_info:
                        if partition_id not in partitions:
                            DiskController._create_partition(partition_id, partition_info[partition_id], disk)
            for disk_name in configuration:
                if disk_name not in disk_names and configuration[disk_name]['state'] not in ['MISSING']:
                    disk = Disk()
                    disk.storagerouter = storagerouter
                    disk.name = disk_name
                    DiskController._update_disk(disk, configuration[disk_name])
                    partition_info = configuration[disk_name]['partitions']
                    for partition_id in partition_info:
                        if partition_info[partition_id]['state'] not in ['MISSING']:
                            DiskController._create_partition(partition_id, partition_info[partition_id], disk)

    @staticmethod
    def _create_partition(partition_id, container, disk):
        """
        Models a partition
        """
        partition = DiskPartition()
        partition.id = partition_id
        partition.disk = disk
        DiskController._update_partition(partition, container)

    @staticmethod
    def _update_partition(partition, container):
        """
        Updates a partition
        """
        for prop in ['filesystem', 'offset', 'state', 'path', 'mountpoint', 'inode', 'size']:
            value = container[prop] if prop in container else None
            setattr(partition, prop, value)
        partition.save()

    @staticmethod
    def _update_disk(disk, container):
        """
        Updates a disk
        """
        for prop in ['vendor', 'state', 'path', 'is_ssd', 'model', 'size']:
            value = container[prop] if prop in container else None
            setattr(disk, prop, value)
        disk.save()

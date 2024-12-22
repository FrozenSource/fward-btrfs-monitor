import os
import sys
import btrfs
import yaml
import subprocess
import time
from datetime import datetime, timedelta
from fward_notifications import *
from fward_env import *
from re import search as re_search

class BtrfsDeviceStats(yaml.YAMLObject):
    yaml_tag = "!BtrfsDeviceStats"
    yaml_loader = yaml.SafeLoader
    def __init__(self, write_errors, read_errors, flush_errors, corruption_errors, generation_errors):
        self.write_errors = write_errors
        self.read_errors = read_errors
        self.flush_errors = flush_errors
        self.corruption_errors = corruption_errors
        self.generation_errors = generation_errors
    
class BtrfsDevice(yaml.YAMLObject):
    yaml_tag = "!BtrfsDevice"
    yaml_loader = yaml.SafeLoader
    def __init__(self, device, uuid, stats):
        self.device = device
        self.uuid = uuid
        self.stats = stats

class BtrfsMountPoint(yaml.YAMLObject):
    yaml_tag = "!BtrfsMountPoint"
    yaml_loader = yaml.SafeLoader
    def __init__(self, mount_point, devices):
        self.mount_point = mount_point
        self.devices = devices

class BtrfsMountChanges(yaml.YAMLObject):
    yaml_tag = "!BtrfsMountChanges"
    yaml_loader = yaml.SafeLoader
    def __init__(self, added_mounts, removed_mounts, added_devices, removed_devices, changed_devices):
        self.added_mounts = added_mounts
        self.removed_mounts = removed_mounts
        self.added_devices = added_devices
        self.removed_devices = removed_devices
        self.changed_devices = changed_devices

# Read /etc/fstab and return a list of all the filesystems that are mounted using btrfs
# We read the lines and then return a list of mount points.
def find_btrfs_mount_points():
    with open('/etc/fstab', 'r') as f:
        lines = f.readlines()
    mount_points = []
    for line in lines:
        if line.startswith('#'):
            continue
        if 'btrfs' in line:
            mount_point = line.split()[1]
            mount_points.append(mount_point)
    return mount_points

def get_all_btrfs_mounts():
    mounts = []
    mount_points = find_btrfs_mount_points()
    for mount_point in mount_points:
        mount = BtrfsMountPoint(mount_point, list())
        with btrfs.FileSystem(mount_point) as fs:
            # Loop over the devices and get their stats.
            for device in fs.devices():
                _info = fs.dev_info(device.devid)
                _stats = fs.dev_stats(device.devid)
                stats = BtrfsDeviceStats(
                    write_errors=_stats.write_errs,
                    read_errors=_stats.read_errs,
                    flush_errors=_stats.flush_errs,
                    corruption_errors=_stats.corruption_errs,
                    generation_errors=_stats.generation_errs
                )
                dev = BtrfsDevice(_info.path, str(_info.uuid), stats)
                mount.devices.append(dev)
        mounts.append(mount)
    return mounts
        
# Function that reads the cache file and returns the data.
# If the file does not exist, it returns None.
# We want to read the yaml back in to structures we can use.
# e.g. BtrfsMountPoint, BtrfsDevice, BtrfsDeviceStats
def read_cache_file(cache_file):
    if not os.path.exists(cache_file):
        return None
    with open(cache_file, 'r') as f:
        data = yaml.safe_load(f)
    return data
    
# Writes cache to a file.
def write_cache_file(cache_file, data):
    with open(cache_file, 'w') as f:
        yaml.dump(data, f)
    
# Function that compares the old and new mount data and returns the Mounts crossed with devices that have different stats.
# It does this using the BtrfsMountChanges dataclass.
# It always returns a BtrfsMountChanges object.
def compare_mounts(old_mounts, new_mounts):
    # Loop over new_mounts and check if their mount_point is in the old_mounts.
    # If not, add them to the added_mounts list.
    added_mounts = [mount for mount in new_mounts if mount.mount_point not in [old_mount.mount_point for old_mount in old_mounts]]
    # Then the removed ones.
    removed_mounts = [mount for mount in old_mounts if mount.mount_point not in [new_mount.mount_point for new_mount in new_mounts]]
    # Then the changed once we do based on the devices.
    # And the device ids.
    # We add the mount as a tuple with the device.
    mount_added_devices = [(mount, device) for mount in new_mounts for device in mount.devices if mount.mount_point in [old_mount.mount_point for old_mount in old_mounts] and device.uuid not in [old_device.uuid for old_mount in old_mounts for old_device in old_mount.devices]]
    # Then the removed ones.
    mount_removed_devices = [(mount, device) for mount in old_mounts for device in mount.devices if mount.mount_point in [new_mount.mount_point for new_mount in new_mounts] and device.uuid not in [new_device.uuid for new_mount in new_mounts for new_device in new_mount.devices]]
    # Lastly we go over each mount point and each device and compare the attribubtes between the old and new mount points based on device id.
    # If the stats are different, we add them to the changed_devices list.
    mount_changed_devices = []
    for new_mount in new_mounts:
        for new_device in new_mount.devices:
            for old_mount in old_mounts:
                if new_mount.mount_point == old_mount.mount_point:
                    for old_device in old_mount.devices:
                        if new_device.uuid == old_device.uuid:
                            # Compare each of the stats.
                            if new_device.stats.write_errors != old_device.stats.write_errors or new_device.stats.read_errors != old_device.stats.read_errors or new_device.stats.flush_errors != old_device.stats.flush_errors or new_device.stats.corruption_errors != old_device.stats.corruption_errors or new_device.stats.generation_errors != old_device.stats.generation_errors:
                                mount_changed_devices.append((new_mount, new_device, old_device))
    return BtrfsMountChanges(added_mounts, removed_mounts, mount_added_devices, mount_removed_devices, mount_changed_devices)

def get_broken_files(mounts, last_check_date_readable, current_date_readable):
    try:
        # Run the journalctl command for the time period
        journalctl_command = [
            'journalctl', '--grep', 'BTRFS warning', '--output', 'cat', '--since', last_check_date_readable, '--until', current_date_readable
        ]

        # Execute the command
        p = subprocess.Popen(journalctl_command, stdout=subprocess.PIPE)
        output, _ = p.communicate()

        # Split the output by newlines, but remove the last empty line
        output = output.decode().strip()

        # Now we are going to get the 'ino' and 'logical' from the output
        inos = []
        logicals = []
        for line in output.split('\n'):
            # Get the device name
            device = re_search(r'device (\w+)', line)
            if not device:
                continue
            
            # First look for the ino
            ino = re_search(r'ino (\d+)', line)
            if ino:
                inos.append((device.group(1), ino.group(1)))
            # Then look for the logical
            logical = re_search(r'logical (\d+)', line)
            if logical:
                logicals.append((device.group(1), logical.group(1)))
                
        # Filter out the duplicates
        inos = list(set(inos))
        logicals = list(set(logicals))
        
        broken_files = []
        
        # Now we are going to grab the file names from the inos
        for [device, ino] in inos:
            # Get the mountpoint for the device
            mount_point = None
            for mount in mounts:
                for dev in mount.devices:
                    if dev.device == ("/dev/" + device):
                        mount_point = mount.mount_point
                        break
                if mount_point:
                    break
            if not mount_point:
                warn(f'Could not find mount point for device: {device}, with a broken file ino: {ino}', None)
                continue
            
            # Run the btrfs inspect-internal inode-resolve command
            btrfs_command = [
                'btrfs', 'inspect-internal', 'inode-resolve', ino, mount_point
            ]
            # if the command returns a non-zero exit code, we skip it
            try:
                p = subprocess.Popen(btrfs_command, stdout=subprocess.PIPE)
                output, _ = p.communicate()
                output = output.decode().strip()
                # Now we are going to get the file name
                file_name = output.split('\n')[-1]
                broken_files.append(file_name)
            except subprocess.CalledProcessError as e:
                error(f'Error while getting broken files: {e}', notifier)
                continue
            
        for [device, logical] in logicals:
            # Get the mountpoint for the device
            mount_point = None
            for mount in mounts:
                for dev in mount.devices:
                    if dev.device == ("/dev/" + device):
                        mount_point = mount.mount_point
                        break
                if mount_point:
                    break
            if not mount_point:
                warn(f'Could not find mount point for device: {device}, with a broken file logical: {logical}', notifier)
                continue
            
            # Run the btrfs inspect-internal logical-resolve command
            btrfs_command = [
                'btrfs', 'inspect-internal', 'logical-resolve', logical, mount_point
            ]
            # if the command returns a non-zero exit code, we skip it
            try:
                p = subprocess.Popen(btrfs_command, stdout=subprocess.PIPE)
                output, _ = p.communicate()
                output = output.decode().strip()
                # Now we are going to get the file name
                file_name = output.split('\n')[-1]
                broken_files.append(file_name)
            except subprocess.CalledProcessError as e:
                error(f'Error while getting broken files: {e}', notifier)
                continue
            
        # Filter out the duplicates
        broken_files = list(set(broken_files))
        # Sort the list
        broken_files.sort()
        return broken_files
    except Exception as e:
        error(f'Error while getting broken files: {e}', None)
        return None
    
# Class to create a lock file
lock_filename = '/tmp/fward.lock'
def lock_file():
    # Try to create the file, if it exists we exit gracefully with a message.
    try:
        lock_fd = os.open(lock_filename, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.close(lock_fd)
    except FileExistsError:
        lock_fd = None
        error(f'Lock file exists {lock_filename}, exiting.', notifier)
        sys.exit(1)

def unlock_file():
    os.remove(lock_filename)

# Main function
if __name__ == '__main__':
    info("Starting Btrfs Monitor v0.2")
    try:
        data_dir = get_environment_variable('FWARD_DATA_DIR', '/var/fward/data')
        create_directory(data_dir)
        check_write_permission(data_dir)
        config_dir = get_environment_variable('FWARD_CONFIG_DIR', '/var/fward/config')
        create_directory(config_dir)
        check_write_permission(config_dir)
        
        cache_file_name = get_environment_variable('FWARD_CACHE_NAME', 'devices.cache')
        cache_file = os.path.join(data_dir, cache_file_name)
        info(f'Cache file: {cache_file}')
        
        notifier_config_name = get_environment_variable('FWARD_NOTIFIER_FILE', 'notifier.conf')
        notifier_config = os.path.join(config_dir, notifier_config_name)
        
        notifier = create_apprise_object(notifier_config)
        if notifier:
            info(f'Notifier config: {notifier_config}')
        else:
            error(f'Could not find notifier config file: {notifier_config}, notifications will not be sent.')
        
        lock_file()
        
        # If the program arguments contain --test-notify, we send a test notification.
        if '--test-notify' in sys.argv:
            if notifier:
                notifier.notify('This is a test notification')
            else:
                print('No notifier found')
            sys.exit(0)
        
        mounts = get_all_btrfs_mounts()
        # Print out all the mounts and devices.
        if '--debug' in sys.argv:
            for mount in mounts:
                print(f'Mount point: {mount.mount_point}')
                for device in mount.devices:
                    print(f'    Device: {device.device} UUID: {device.uuid}')
                    print(f'        Write errors: {device.stats.write_errors}')
                    print(f'        Read errors: {device.stats.read_errors}')
                    print(f'        Flush errors: {device.stats.flush_errors}')
                    print(f'        Corruption errors: {device.stats.corruption_errors}')
                    print(f'        Generation errors: {device.stats.generation_errors}')
        # If there are no mounts, say so
        if not mounts:
            warn('No btrfs mounts found', notifier)
            sys.exit(0)
        
        # Read old cache if it exists
        old_mounts = read_cache_file(cache_file)    
        
        write_cache_file(cache_file, mounts)
        
        if old_mounts is None:
            warn('No old cache found', notifier)
            sys.exit(0)
        else:
            # Compare the old and new mounts
            changes = compare_mounts(old_mounts, mounts)
            if changes.added_mounts:
                for mount in changes.added_mounts:
                    info(f'Added mount: {mount.mount_point}', notifier)
            if changes.removed_mounts:
                for mount in changes.removed_mounts:
                    warn(f'Removed mount: {mount.mount_point}', notifier)
            if changes.added_devices:
                for mount, device in changes.added_devices:
                    info(f'Added device: {device.device} to {mount.mount_point}', notifier)
            if changes.removed_devices:
                for mount, device in changes.removed_devices:
                    warn(f'Removed device: {device.device} from {mount.mount_point}', notifier)
                    
            if changes.changed_devices:
                for mount, new_device, old_device in changes.changed_devices:
                    error(f'Changed stats of device: {new_device.device} in {mount.mount_point}\nWrite errors: {old_device.stats.write_errors} -> {new_device.stats.write_errors}\nRead errors: {old_device.stats.read_errors} -> {new_device.stats.read_errors}\nFlush errors: {old_device.stats.flush_errors} -> {new_device.stats.flush_errors}\nCorruption errors: {old_device.stats.corruption_errors} -> {new_device.stats.corruption_errors}\nGeneration errors: {old_device.stats.generation_errors} -> {new_device.stats.generation_errors}', notifier)
            # if none of the above are true, we are done.
            if not changes.added_mounts and not changes.removed_mounts and not changes.added_devices and not changes.removed_devices and not changes.changed_devices:
                info('No changes in mounts or devices.')
            else:
                info('Changes found in mounts or devices.', notifier)

        # Now we are going to check for broken files.
        FWARD_LAST_CHECK_FILE = get_environment_variable('FWARD_LAST_CHECK_FILE', os.path.join(data_dir, 'last_check'))

        # Get the current date
        current_date = int(time.time())

        # Check if the last check date file exists
        if os.path.exists(FWARD_LAST_CHECK_FILE):
            with open(FWARD_LAST_CHECK_FILE, 'r') as file:
                last_check_date = int(file.read().strip())
        else:
            # If the file doesn't exist, initialize it with 1/1/1970
            last_check_date = 0

        # Convert dates to readable format for journalctl
        last_check_date_readable = datetime.fromtimestamp(last_check_date).strftime('%Y-%m-%d %H:%M:%S')
        current_date_readable = datetime.fromtimestamp(current_date).strftime('%Y-%m-%d %H:%M:%S')

        # Store the current date for the next run
        with open(FWARD_LAST_CHECK_FILE, 'w') as file:
            file.write(str(current_date))
            
        # Run the journalctl command for the time period
        broken_files = get_broken_files(mounts, last_check_date_readable, current_date_readable)
        if broken_files:
            list = '\n'.join(broken_files)
            raise Exception(f'Broken files detected:\n{list}')
            
        unlock_file()
        info("Nothing broken detected")
        sys.exit(0)
    except Exception as e:
        unlock_file()
        error(f'{e}', notifier)
        sys.exit(0)
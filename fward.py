import os
import sys
import btrfs
import yaml
import apprise
from apprise import NotifyType
from dataclasses import dataclass
    
global notifier

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

# Function that tries to create a directory if it does not exist.
# If it does exist, it does nothing.
# If it fails to create the directory, it exits the program.
def create_directory(directory):
    try:
        os.makedirs(directory)
    except FileExistsError:
        pass
    except Exception as e:
        print(f'Failed to create directory {directory}: {e}')
        sys.exit(1)
        
# Function that tries to read from environment variable and returns the value.
# Otherwise it uses the provided default value.
def get_environment_variable(name, default):
    try:
        return os.environ[name]
    except KeyError:
        return default
    
# Function that checks whether I can write to a directory.
# It tries to create a file and then delete it.
# If it fails, it exits the program.
# And writes a message to the user.
def check_write_permission(directory):
    try:
        with open(os.path.join(directory, 'write.lock'), 'w') as f:
            f.write('write.lock')
        os.remove(os.path.join(directory, 'write.lock'))
    except Exception as e:
        print(f'Failed to write to directory {directory}: {e}')
        sys.exit(1)
        
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

def create_apprise_object(configfile):
    try:
        apobj = apprise.Apprise()
        # Read the configuration file
        # And add on split lines.
        with open(configfile, 'r') as f:
            lines = f.readlines()
        for line in lines:
            apobj.add(line.strip())
        return apobj
    except Exception as e:
        print(f'Failed to create apprise object: {e}')
        return None

# Function that logs a message.
# It will also do a syslog if it is available.
def info(message, notifier=None):
    print("[FWARD][INFO] " + message)
    # Try to log to syslog
    try:
        import syslog
        syslog.syslog(syslog.LOG_INFO, message)
    except:
        pass
    if notifier:
        notifier.notify(
                body=message,
                title='[LOG] Btrfs Monitor',
                notify_type=NotifyType.INFO
            )
def error(message, notifier=None):
    print("[FWARD][ERROR] " + message)
    # Try to log to syslog
    try:
        import syslog
        syslog.syslog(syslog.LOG_ERR, message)
    except:
        pass
    if notifier:
        notifier.notify(
                body=message,
                title='[ERROR] Btrfs Monitor',
                notify_type=NotifyType.FAILURE
            )
def warn(message, notifier=None):
    print("[FWARD][WARN] " + message)
    # Try to log to syslog
    try:
        import syslog
        syslog.syslog(syslog.LOG_WARNING, message)
    except:
        pass
    if notifier:
        notifier.notify(
                body=message,
                title='[WARNING] Btrfs Monitor',
                notify_type=NotifyType.WARNING
            )

# Main function
if __name__ == '__main__':
    info("Starting Btrfs Monitor v0.1")
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
        info('Done, nothing to report.')
    else:
        info('Done, reported changes.')
    
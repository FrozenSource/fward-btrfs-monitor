import os
import sys
import subprocess
import time
from datetime import datetime, timedelta
from fward_notifications import *
from fward_env import *

def get_broken_files(last_check_date_readable, current_date_readable):
    try:
        # Run the journalctl command for the time period
        journalctl_command = [
            'journalctl', '--output', 'cat', '--since', last_check_date_readable, '--until', current_date_readable
        ]
        grep_command = ['grep', '-e', 'BTRFS warning.*path:']
        sed_command = ['sed', '-e', 's/^.*path: //', '-e', 's/)$//']
        sort_command = ['sort']
        uniq_command = ['uniq']

        # Execute the commands
        p1 = subprocess.Popen(journalctl_command, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(grep_command, stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        p3 = subprocess.Popen(sed_command, stdin=p2.stdout, stdout=subprocess.PIPE)
        p2.stdout.close()
        p4 = subprocess.Popen(sort_command, stdin=p3.stdout, stdout=subprocess.PIPE)
        p3.stdout.close()
        p5 = subprocess.Popen(uniq_command, stdin=p4.stdout, stdout=subprocess.PIPE)
        p4.stdout.close()

        output, _ = p5.communicate()
        output = output.decode().strip()
        
        # Split the output by newlines, but remove the last empty line
        arr = output.split('\n')
        if arr[-1] == '':
            arr = arr[:-1]
            
        if len(arr) == 0:
            return None        
        return arr
    except:
        return None
    
 # Function to sync files using rsync with -raI options
def sync_file(dest_file, src_file):
    # Ensure the source file exists
    if not os.path.exists(src_file):
        error(f"file {src_file} does not exist, cannot sync.", notifier)
        return False
    
    # Ensure the destination directory exists
    dest_dir = dest_file
    if os.path.isfile(src_file):
        dest_dir = os.path.dirname(dest_file)
    os.makedirs(dest_dir, exist_ok=True)

    try:
        # Run rsync to sync the file
        rsync_command = ['rsync', '-raI', src_file, dest_file]
        subprocess.run(rsync_command, check=True)
        return True
    except subprocess.CalledProcessError as e:
        error(f"rsync failed for {broken_file}. Exception: {e}", notifier)
        return False
    
def is_mount_point(directory):
    # Get the device number of the directory
    dir_stat = os.stat(directory)
    dir_dev = dir_stat.st_dev

    # Get the device number of the parent directory (to detect root filesystem)
    parent_dev = os.stat(os.path.dirname(directory)).st_dev

    # Compare device numbers to check if it's a mount point
    return dir_dev != parent_dev

# Main function
if __name__ == '__main__':
    info("Starting FWARD Auto-Restorer v0.1")
    config_dir = get_environment_variable('FWARD_CONFIG_DIR', '/var/fward/config')
    create_directory(config_dir)
    check_write_permission(config_dir)
    
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
    
    # Ensure FWARD_SYNC_SRC_DIR and FWARD_SYNC_BACKUP_DIR are set
    FWARD_SYNC_SRC_DIR = os.getenv('FWARD_SYNC_SRC_DIR')
    FWARD_SYNC_BACKUP_DIR = os.getenv('FWARD_SYNC_BACKUP_DIR')

    FWARD_SYNC_LAST_CHECK_FILE = get_environment_variable('FWARD_SYNC_LAST_CHECK_FILE', 'last_check_date.txt')

    # Get the current date
    current_date = int(time.time())

    # Check if the last check date file exists
    if os.path.exists(FWARD_SYNC_LAST_CHECK_FILE):
        with open(FWARD_SYNC_LAST_CHECK_FILE, 'r') as file:
            last_check_date = int(file.read().strip())
    else:
        # If the file doesn't exist, initialize it with the current date minus 10 minutes
        last_check_date = int((datetime.now() - timedelta(minutes=10)).timestamp())

    # Convert dates to readable format for journalctl
    last_check_date_readable = datetime.fromtimestamp(last_check_date).strftime('%Y-%m-%d %H:%M:%S')
    current_date_readable = datetime.fromtimestamp(current_date).strftime('%Y-%m-%d %H:%M:%S')

    # Print the list for verification
    if not is_mount_point(FWARD_SYNC_BACKUP_DIR):
        info(f"{FWARD_SYNC_BACKUP_DIR} is not a mount point.")
        FWARD_SYNC_BACKUP_DIR = None

    # Run the journalctl command for the time period
    broken_files = get_broken_files(last_check_date_readable, current_date_readable)
    if not broken_files:
        info("No broken files were detected.")
        sys.exit(0)
        
    if FWARD_SYNC_SRC_DIR and FWARD_SYNC_BACKUP_DIR:
        info("Files have been found broken, restoring the files...", notifier)
    else:
        warn("Skipping restoration of files as FWARD_SYNC_SRC_DIR or FWARD_SYNC_BACKUP_DIR is not set.", notifier)
    
    all_successful = True
    for broken_file in broken_files:
        if broken_file:
            info(f"{broken_file}...", notifier)
            if not FWARD_SYNC_SRC_DIR or not FWARD_SYNC_BACKUP_DIR:
                continue
            
            # Check if the file was in the source directory
            src_file = os.path.join(FWARD_SYNC_SRC_DIR, broken_file)
            dest_file = os.path.join(FWARD_SYNC_BACKUP_DIR, broken_file)
            
            if not os.path.exists(src_file):
                # Check if the file was in the backup directory
                if os.path.exists(dest_file):
                    warn(f"Skipping {broken_file} as it is not in the source or directory, but exists in the backup.", notifier)
                else:
                    warn(f"Skipping {broken_file} as it is not in the source or backup directory.", notifier)
                all_successful = False
                continue
            
            if not sync_file(dest_file, src_file):
                error(f"Failed to synchronize {broken_file}", notifier)
                all_successful = False
    if not all_successful:
        error("Some broken files could not be restored.", notifier)
        sys.exit(1)
    info("All broken files have been restored successfully.", notifier)
        
    # Store the current date for the next run
    with open(FWARD_SYNC_LAST_CHECK_FILE, 'w') as file:
        file.write(str(current_date))
        
    sys.exit(0)
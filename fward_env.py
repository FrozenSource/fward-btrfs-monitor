import os
import sys
import apprise
from apprise import NotifyType
from dataclasses import dataclass

global notifier

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
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
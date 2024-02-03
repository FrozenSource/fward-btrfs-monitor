# fward-btrfs-monitor
**Monitors btrfs mounts and device stats, and reports should that be needed.**

The program will keep track of all mounts for you and report them to stdout, syslog, and the other notifiers if there are any changes at all.
Changes meaning mounts added or removed, devices added or removed, and stats of these devices changed.

For notifying outside of syslog and stdout fward uses [apprise](https://github.com/caronc/apprise).. A config file by default: ```/var/fward/config/notifiers.conf```(FWARD_NOTIFIER_FILE).

It is recommended to add fward to for example crontab like this:
```
# Example of job definition:
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed
* * * * * root /usr/local/bin/fward
 ```



# Build Executable
make.sh can be called to make a singular executable.

# Command Line
The base will always scan and report.
## --test-notify
**description:** will send a notification using apprise. See FWARD_NOTIFIER_FILE @ Environment Variables. After sending will exit.
## --debug
**description:** will do base behaviour and also print all mount points and devices it found.
# Environment Variables
## FWARD_CONFIG_DIR
**default:** /var/fward/config<br>
**description:** Location of the config directory.
## FWARD_DATA_DIR
**default:** /var/fward/data<br>
**description:** Location of the data directory.
## FWARD_CACHE_NAME
**default:** devices.cache<br>
**description:** Located in the data directory, this file contains the cache of the filesystem stats. You can even change one to make sure it works.
## FWARD_NOTIFIER_FILE
**default:** notifier.conf<br>
**description:** Located in the config directory, this file contains notifiers according to [Apprise](https://github.com/caronc/apprise).

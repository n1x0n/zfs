ZFS scripts and tools
======

This repository is used to store some scripts and tools used to manage ZFS on Solaris. Everything here is provided as-is and should be used carefully.

update_snapshot.pl
----

This script is run by cron every minute to take snapshots of the specified filesystem at regular intervals. The script also removes old snapshots according to the specified schedule.
 
The defaults are:

- One snapshot every 10 minutes for the last 2 hours.
- Hourly snapshots, 24 hours retention.
- Daily snapshots, 7 days retention.
- Weekly snapshots, 12 weeks retention.
- Montly snapshots, 12 months retention.
- Yearly snapshots, kept forever. 

The script has been profiled and tuned to be able to manage thousands of snapshots.

Here is how I run it in crontab:
    * * * * * /root/zfs/update_snapshot.pl --filesystem tank/zones --weekly 5 --monthly 12 --yearly -1 --recursive

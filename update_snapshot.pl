#!/usr/bin/perl -wT

use strict;
$ENV{PATH}='';
$ENV{http_proxy}='';
my $DEBUG=0;



############################################################
# Modules
############################################################
use POSIX qw(strftime mktime);




############################################################
# Variables 
############################################################
my $ZFS = "/usr/sbin/zfs";
my $tag = "GMT-" . (strftime "%Y.%m.%d-%H.%M.%S", localtime);
my $lockfile = "/root/.update_snapshot/update_snapshot.lock";
my $snapshotlist = '';
my $fullshotlist = '';
my $pid = $$;
my @filesystemlist;



############################################################
# int main(void) {}
############################################################
# Get options
my %options = %{(&get_options)};

# Do we need to take action?
if ( &take_action ) {
	# Yes, get lock.
	my $waittime = ( $options{'opt_interval'} * 60 ) - 10;
	my $counter = 0;
	while ( $counter < $waittime ) {
		&debug("$counter / $waittime");
		last if ( &get_lock );
		$counter += 1;
		sleep 1;
	}
	if ( $counter >= $waittime ) {
		# Timed out
		&debug("timed out");
		exit 0;
	}
	&debug("ready for action");
} else {
	# No action needed
	&debug("No action needed.");
	exit(0);
}

# Take snapshots
&get_recursive if ( $options{'opt_recursive'} ); 
&new_snapshot($options{'opt_fs'});
foreach my $this_filesystem ( @filesystemlist ) {
	$snapshotlist = '';
	&cleanup_snapshots($this_filesystem);
}

# Drop lock
&drop_lock;


############################################################
# Subroutines
############################################################


sub get_options {
    my %options = (
        'opt_fs'          => '',
        'opt_help'        => '',
        'opt_recursive'	  => '',
        'opt_interval'    => 10,
        'often'       => 12,
        'hourly'      => 24,
        'daily'       => 7,
        'weekly'      => 12,
        'monthly'     => 12,
        'yearly'      => -1,
    );

    #my $opt_interval    = 5;  # Snapshot every 5 minutes.
    #my $opt_often       = 12; # Keep 12 snapshots taken every interval.
    #my $opt_hourly      = 24; # Keep 24 hourly snapshots.
    #my $opt_daily       = 7;  # Keep 7 daily snapshots.
    #my $opt_weekly      = 12; # Keep 12 weekly snapshots.
    #my $opt_monthly     = 12; # Keep 12 monthly snapshots.
    #my $opt_yearly      = -1; # Keep infinite yearly snapshots.

    use Getopt::Long;
    my $result = GetOptions (
        "help"          => \$options{'opt_help'},
        "recursive"     => \$options{'opt_recursive'},
        "filesystem=s"  => \$options{'opt_fs'},
        "interval=i"    => \$options{'opt_interval'},
        "often=i"       => \$options{'often'},
        "hourly=i"      => \$options{'hourly'},
        "daily=i"       => \$options{'daily'},
        "weekly=i"      => \$options{'weekly'},
        "monthly=i"     => \$options{'monthly'},
        "yearly=i"      => \$options{'yearly'},
    );

    &debug("Filesystem: ->$options{'opt_fs'}<-");

    if ( $options{'opt_help'} || ! $options{'opt_fs'} ) {
        &usage;
        &quit;
    }

    my $verified = &verify_fs($options{'opt_fs'});
    unless ( $verified ) {
        &abort(qq|No such filesystem: $options{'opt_fs'}|);
    } else {
        $options{'opt_fs'} = $verified;
        push @filesystemlist, $options{'opt_fs'};
    }

    return(\%options);
}


sub take_action {
    my $minutes = $tag;
    $minutes =~ s/\.[0-9][0-9]$//;
    $minutes =~ s/^.*\.//;
    if ( ($minutes - 1) % $options{'opt_interval'} ) {
	# No action needed
	return 0;
    } else {
	# Take action
	return 1;
    }
}


sub get_lock {
	my $return_value = 0;
	if ( -f $lockfile ) {
		&debug("found file");
	} else {
		&debug("No lock found. Taking lock.");
		open (LOCKFILE, ">$lockfile") or die qq|Cannot create lockfile "$lockfile": $!\n|;
		print LOCKFILE "$pid"; 
		close LOCKFILE;
		&debug("Waiting for others.");
		sleep 1;
		&debug("Reading lock.");
		open (LOCKFILE, "$lockfile") or die qq|Cannot read lockfile "$lockfile": $!\n|;
		my $found_pid = <LOCKFILE>;
		close LOCKFILE;
		if ( $found_pid =~ /^\s*${pid}\s*$/ ) {
			&debug("We have the lock");
			$return_value = 1;
		} else {
			&debug("Someone else got the lock");
		}
	}
	return($return_value);
}


sub drop_lock {
	&debug("Dropping lock.");
	&debug(qq|Removing lockfile "$lockfile".|);
	system(qq|/usr/gnu/bin/rm -f "$lockfile"|);
}


sub new_snapshot {
    my $this_filesystem = shift;
    my $minutes = $tag;
    $minutes =~ s/\.[0-9][0-9]$//;
    $minutes =~ s/^.*\.//;
    my $recursive = "";
    $recursive = "-r" if $options{'opt_recursive'};
    unless ( ($minutes - 1) % $options{'opt_interval'} ) {
        my $cmd = "$ZFS snapshot $recursive $this_filesystem\@$tag"; 
        #my $cmd = "$ZFS snapshot $this_filesystem\@$tag"; 
        open (CMD, "$cmd |") || &abort(qq|Cannot run command "$cmd": $!|);
        while (<CMD>) {
            &debug($_);
        }
        close CMD;
    }
}


sub cleanup_snapshots {
    my $this_filesystem = shift;
    &debug(qq|Cleaning up snapshots for "$this_filesystem"|);
    &init_snapshotlist($this_filesystem);
    my @snaplist = reverse sort keys %{$snapshotlist};
    my %typehash;
    foreach my $thissnap ( @snaplist ) {
        my $hit = 0;
        foreach my $thistype ( qw( yearly monthly weekly daily hourly often) ) {
            if ( &istype($thistype, $thissnap) ) {
                if ( $typehash{$thistype} ) {
                   $typehash{$thistype} += 1;
                } else {
                    $typehash{$thistype} = 1;
                }
                unless ( $typehash{$thistype} > $options{$thistype} && $options{$thistype} >= 0 ) {
                    if ( $options{$thistype} ) {
                        $hit = 1;
                    }
                }
            }
        }
        if ( $hit ) {
            # Keep this snapshot.
            &debug("Keeping $thissnap");
        } else {
            &debug("Removing $thissnap");
            my $cmd = "$ZFS destroy $thissnap"; 
            open (CMD, "$cmd |") || &abort(qq|Cannot run command "$cmd": $!|);
            while (<CMD>) {
                &debug($_);
                print "$_";
            }
            close CMD;
        }
    }
}


sub istype {
    my $thistype = shift;
    my $thissnap = shift;

    if ( $thistype eq "yearly" ) {
        # Is this the first minute of the first day of the year?
        if ( $thissnap =~ /\@GMT-[0-9]{4}\.01\.01-00\.01\./ ) {
            return 1;
        } else {
            return 0;
        }
    }

    if ( $thistype eq "monthly" ) {
        # Is this the first minute of the first day of the month?
        if ( $thissnap =~ /\@GMT-[0-9]{4}\.[0-9]{2}\.01-00\.01\./ ) {
            return 1;
        } else {
            return 0;
        }
    }

    if ( $thistype eq "daily" ) {
        # Is this the first minute of the day?
        if ( $thissnap =~ /\@GMT-[0-9]{4}\.[0-9]{2}\.[0-9]{2}-00\.01\./ ) {
            return 1;
        } else {
            return 0;
        }
    }

    if ( $thistype eq "hourly" ) {
        # Is this the first minute of the hour?
        if ( $thissnap =~ /\@GMT-[0-9]{4}\.[0-9]{2}\.[0-9]{2}-[0-9]{2}\.01\./ ) {
            return 1;
        } else {
            return 0;
        }
    }

    if ( $thistype eq "weekly" ) {
        my $time_t;
        if ( $thissnap =~ /\@GMT-([0-9]{4})\.([0-9]{2})\.([0-9]{2})-00\.01\./ ) {
            $time_t = mktime(0,0,1,$3,($2-1),($1-1900));
        } else {
            return 0;
        }
        my $weekday = strftime "%u", localtime($time_t);
        if ($weekday == 1) {
            # Weekday = 1 means Monday.
            return 1;
        } else {
            return 0;
        }
    }

    if ( $thistype eq "often" ) {
        # Is this hitting the interval?
        if ( $thissnap =~ /\@GMT-[0-9]{4}\.[0-9]{2}\.[0-9]{2}-[0-9]{2}\.([0-9]{2})\./ ) {
            my $minutes = $1;
            unless ( ($minutes - 1) % $options{'opt_interval'} ) {
                return 1;
            }
        } else {
            # Should never happen, but just in case.
            return 0;
        }
    }

    return(0);
}


sub init_snapshotlist {
    return if ($snapshotlist);
    &debug("Initializing snapshot list");
    &init_fullshotlist;

    my %thislist;
    my $this_filesystem = shift;

    foreach my $thisrow (sort keys %{$fullshotlist}) {
        next unless ($thisrow =~ /^($this_filesystem\@GMT-.*?)$/ );
        $thislist{$1} = 1;
    }
    close CMD;
    $snapshotlist = \%thislist;
    &debug("Found " . (scalar keys %thislist) . " snapshots.");
}


sub init_fullshotlist {
    return if ($fullshotlist);
    &debug("Initializing FULL snapshot list");
    my %thislist;
    my $this_filesystem = $options{'opt_fs'};

    my $cmd = "$ZFS list -t snapshot -r $options{'opt_fs'}";
    &debug($cmd);
    open (CMD, "$cmd |") || &abort(qq|Cannot run command "$cmd": $!|);
    while (my $thisrow = <CMD>) {
        next unless ($thisrow =~ /^\s*($this_filesystem.*\@GMT-.*?)\s+/ );
        $thislist{$1} = 1;
    }
    close CMD;
    $fullshotlist = \%thislist;
    &debug("Found " . (scalar keys %thislist) . " snapshots for FULL.");
}


sub verify_fs {
    my $dirty_fs = shift;
    my $this_fs = "thisshouldbechangedbelow";
    if ( $dirty_fs =~ /^(.*)$/ ) {
        $this_fs = $1;
    }
   
    my $cmd = "$ZFS list -t filesystem $this_fs";
    &debug($cmd);

    open (CMD, "$cmd |") || &abort(qq|Cannot run command "$cmd": $!|);

    my $found = '';
    while (my $thisrow = <CMD>) {
        next unless ( $thisrow =~ /^\s*($this_fs)\s+/ );
        $found = $1;
        next unless ( $found =~ /^(.*)$/ );
        $found = $1;
    }

    close CMD;

    return($found);
}


sub get_recursive {
    my $this_fs = $filesystemlist[0];
    my $cmd = "$ZFS list -t filesystem -r $this_fs";

    &debug("Recursive search for $this_fs.");

    open (CMD, "$cmd |") || &abort(qq|Cannot run command "$cmd": $!|);

    while (my $thisrow = <CMD>) {
        next unless ( $thisrow =~ /^\s*(${this_fs}.*?)\s+/ );
	my $found = $1;
        next if ( $found =~ /^${this_fs}$/ );
        push @filesystemlist, $found;
	debug("Found $found");
    }

    close CMD;

    return(1);
}


sub usage {
    print "Usage: update_snapshot.pl --filesystem FILESYSTEM\n";
    print "       [--interval 10] [--often 12] [--hourly 24]\n";
    print "       [--daily 7] [--weekly 12] [--monthly 12]\n";
    print "       [--yearly -1] [--recursive]\n";
    print "\n";
    print "Default values above. Use '0' to disable a type of snapshot,\n";
    print "use '-1' to keep for ever.\n";
}


sub debug {
    return unless ($DEBUG);
    my $message = join '::', @_;
    if ( $message ) {
        print STDERR "DEBUG: $message\n";
    } else {
        print STDERR "DEBUG: No message specified.\n";
    }
}


sub cleanup {
    # Empty for now.
    1;
}


sub quit {
    &cleanup;
    exit(0);
}


sub abort {
    my $message = $_[0];
    if ( $message ) {
        print STDERR "Aborting: $message\n";
    } else {
        print STDERR "Aborting!\n"
    }
    &cleanup;
    exit(1);
}

#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use GRNOC::Simp::Pusher;

sub usage {
    print "$0 [--config <config file>] [--logging <log config>] [--nofork]\n";
    exit( 1 );
}

use constant DEFAULT_CONFIG_FILE => "/etc/grnoc/simp/pusher-config.xml";

my $config_file = DEFAULT_CONFIG_FILE;
my $logging;
my $nofork = 0;
my $help;

GetOptions( "config|c=s" => \$config_file,
	    "logging=s" => \$logging,
            "nofork" => \$nofork,
            "help|h|?" => \$help,
    );


usage() if ( $help );

my $pusher = GRNOC::Simp::Pusher->new(
    config_file => $config_file,
    logging_file => $logging,
    daemonize => !$nofork
    );

$pusher->start();


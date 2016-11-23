package GRNOC::Simp::Pusher::Worker;

use strict;
use warnings;

use Moo;
use AnyEvent;
use Data::Dumper;

use GRNOC::RabbitMQ::Client;
use GRNOC::WebService::Client;

has worker_name => ( is => 'ro',
                required => 1 );

has config      => ( is => 'ro',
               required => 1 );


has logger => ( is => 'ro',
                required => 1 );

has hosts => ( is => 'ro',
               required => 1 );

has is_running => ( is => 'rwp',
                    default => 0 );

has need_restart => (is => 'rwp',
		    default => 0 );

has rabbit => ( is => 'rwp' );

has wsc => ( is => 'rwp' );

sub start {
    my ($self) = @_;

    my $worker_name = $self->worker_name;

    $self->logger->debug("Starting.");

    $self->_set_is_running(1);

    $0 = "simp($worker_name)";

    $SIG{'TERM'} = sub {
	$self->logger->info("Received SIGTERM.");
    };

    $SIG{'HUP'} = sub {
	$self->logger->info("Received SIGHUP.");
    };

    my $wsc_url = $self->config->get('/config/tsds/@url');
    my $wsc_user = $self->config->get('/config/tsds/@user');
    my $wsc_pass = $self->config->get('/config/tsds/@pass');

    my $rabbit_host = $self->config->get('/config/rabbitMQ/@host')->[0];
    my $rabbit_port = $self->config->get('/config/rabbitMQ/@port')->[0];
    my $rabbit_user = $self->config->get('/config/rabbitMQ/@user')->[0];
    my $rabbit_pass = $self->config->get('/config/rabbitMQ/@password')->[0];

    my $wsc = GRNOC::WebService::Client->new(
	url => $wsc_url,
	uid => $wsc_user,
	passwd => $wsc_pass,
	usePost => 1
	);

    my $rabbit = GRNOC::RabbitMQ::Client->new(
	topic => "Simp.Data",
	exchange => "Simp",
	user => $rabbit_user,
	pass => $rabbit_pass,
	host => $rabbit_host,
	port => $rabbit_port
	);

    $self->_set_wsc($wsc);
    $self->_set_rabbit($rabbit);
    $self->_set_need_restart(0);

    $self->logger->debug("Starting polling loop.");

    return $self->_loop();
}

sub _loop {
    my ($self) = @_;
    my $hosts = $self->hosts;

    my $ips = [];
    foreach my $host (@$hosts) {
	push(@$ips, $host->{'ip'});
    }

    my $cv = AnyEvent->condvar();
    $self->rabbit->get(
    	oidmatch => "1.3.6.1.2.1.2.2.1.2.*",
    	ipaddrs => $ips,
    	ipaddrs => ["157.91.30.4", "157.91.30.3"],
    	async_callback => sub {
    	    my $res = shift;
    	    $cv->send($res);
    	});
    my $intf_names = $cv->recv();
    $self->logger->debug(Dumper($intf_names));

    $cv = AnyEvent->condvar();
    $self->rabbit->get(
    	oidmatch => "1.3.6.1.4.1.2636.3.15.1.1.11.*",
    	ipaddrs => $ips,
    	async_callback => sub {
    	    my $res = shift;
    	    $cv->send($res);
    	});
    my $queue_drops = $cv->recv();
    $self->logger->debug(Dumper($queue_drops));

    # my $data = [];
    # foreach my $node_ip (keys %{$intf_names->{'results'}}) {
    # 	my $node = {};
    # 	$node->{'ip'} = $node_ip;
    # 	$node->{'intfs'} = [];
    # 	my $intf = {};
	
    # 	$self->logger->debug(Dumper($node));
    # }
    
}
1;

package GRNOC::Simp::Pusher;

use strict;
use warnings;

use Moo;
use Types::Standard qw(Str Bool);
use Proc::Daemon;
use Parallel::ForkManager;
use Data::Dumper;

#use Math::Round qw( nhimult );
#use JSON;

use GRNOC::Config;
use GRNOC::Log;
#use GRNOC::Counter;
#use GRNOC::WebService::Client;
use GRNOC::Simp::Pusher::Worker;

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );
has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has children => ( is => 'rwp',
                  default => sub { [] } );
sub BUILD {
    my ( $self ) = @_;

    my $grnoc_log = GRNOC::Log->new(config => $self->logging_file);
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger($logger);

    my $config = GRNOC::Config->new(config_file => $self->config_file,
				    force_array => 1);
    $self->_set_config($config);

    return $self;
}


# use constant DEFAULT_PID_FILE => '/var/run/mpls-lsp-usage-collector.pid';
# my $counter;
# my $interval;
# my $push_svc;
# my $update_svc;
# my %counter_keys = ();

sub start {
    my ($self) = @_;

    $self->logger->info("Starting.");
    $self->logger->debug("Setting up signal handlers.");

    $SIG{'TERM'} = sub {
	$self->logger->info("Received SIGTERM.");
	$self->stop();
    };

    $SIG{'HUP'} = sub {
	$self->logger->info("Received SIGHUP.");
    };

    if ($self->{'daemonize'}) {
	$self->logger->debug("Daemonizing.");

	my $daemon = Proc::Daemon->new(
	    pid_file => $self->config->get('/config/pid-file')
	    );

	my $pid = $daemon->init();

	if (!$pid) {
	    $self->logger->debug("Created daemon process.");

	    $0 = "simpPusher";

	    $self->_create_workers();
	}
    } else {
	$self->logger->debug("Running in foreground.");
	$self->_create_workers();
    }

    return 1;
}

sub stop {
    my ($self) = @_;

    $self->logger->info("Stopping.");
    my @pids = @{$self->children};
    $self->logger->debug("Stopping child worker processes " . join(' ', @pids) . ".");
    return kill('TERM', @pids);
}

sub _create_workers {
    my ($self) = @_;


    my $groups = $self->config->get("/config/group");
    $self->logger->debug(Dumper($groups));
#    my $forker = Parallel::ForkManager->new(10);

    foreach my $group (@$groups) {
	next if ($group->{'active'} == 0);
	my $name = $group->{'name'};
	my $workers = $group->{'workers'};

	my $hosts = [];
	foreach my $host (@{$group->{'host'}}) {
	    push(@$hosts, $host);
	}

	$self->logger->info("Creating $workers child processes for group: $name");

	# $forker->run_on_start( sub {
	#     my ($pid) = @_;
	#     $self->logger->debug("Child worker process $pid created.");
	#     push(@{$self->children}, $pid);
	# 		       });

	
#	$forker->start() and next; 

	my $worker = GRNOC::Simp::Pusher::Worker->new(
	    worker_name => $name,
	    config => $self->config,
	    hosts => $hosts,
	    logger => $self->logger);

	$worker->start();

#	$forker->finish();
    }
}

1;

# sub _run {
#     my ($self) = @_;

#     log_info("Starting");

#     while ($self->{'running'}) { 
# 	my $now = time();
# 	my $timestamp = nhimult($interval, $now);
# 	my $sleep_seconds = $timestamp - $now;
# 	my $human_readable_date = localtime($timestamp);

# 	log_info("Sleeping $sleep_seconds until local time $human_readable_date ($timestamp).");

# 	while ($sleep_seconds > 0) {
# 	    my $time_slept = sleep($sleep_seconds);
# 	    last if (!$self->{'running'});
# 	    $sleep_seconds -= $time_slept;
# 	}

# 	last if (!$self->{'running'});

# 	if ($self->{'hup'}) { 
# 	    log_info("Handle HUP");
# 	    $self->_init();
# 	    log_info("HUP finished");
# 	    $self->{'hup'} = 0;
# 	    next;
# 	}
# 	$self->_collect();
#     }
# }

# sub _submit_data {
#     my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data) = @_;
#     my $node_name = $ident;

#     if ($data) {
#     	my $timestamp = $data->{'timestamp'};
# 	my $stats = $data->{'stats'};

#     	foreach my $lsp (keys(%{$stats})) {
#     	    my $key = "$node_name|$lsp|octets";
#     	    if (!exists($counter_keys{$key})) {
#     		$counter_keys{$key} = 1;
#     		$counter->add_measurement($key, $interval, 0, MAX_RATE_VALUE);
#     	    }

#     	    my $octet_rate = $counter->update_measurement($key, $timestamp, $stats->{$lsp}->{'octets'});
#     	    $octet_rate = ($octet_rate >= 0) ? $octet_rate : undef;

#     	    $key = "$node_name|$lsp|packets";
#     	    if (!exists($counter_keys{$key})) {
#     		$counter_keys{$key} = 1;
#     		$counter->add_measurement($key, $interval, 0, MAX_RATE_VALUE);
#     	    }

#     	    my $packet_rate = $counter->update_measurement($key, $timestamp, $stats->{$lsp}->{'packets'});
#     	    $packet_rate = ($packet_rate >= 0) ? $packet_rate : undef;
	    
#     	    my $msg = {};
#     	    $msg->{'type'} = 'lsp';
#     	    $msg->{'time'} = $timestamp;
#     	    $msg->{'interval'} = $interval;
	
#     	    my $meta = {};
#     	    $meta->{'node'} = $node_name;
#     	    $meta->{'lsp'} = $lsp;
#     	    $msg->{'meta'} = $meta;
	    
#     	    my $values = {};
#     	    $values->{'state'} = $stats->{$lsp}->{'state'};
#     	    $values->{'bps'} = (defined $octet_rate) ? $octet_rate * 8 : undef;
#     	    $values->{'pps'} = $packet_rate;
#     	    $msg->{'values'} = $values;

#     	    my $tmp = [];
#     	    push(@$tmp, $msg);
#     	    my $json_push = encode_json($tmp);
#     	    my $res_push = $push_svc->add_data(data => $json_push);
#     	    if (!defined $res_push) {
#     		log_error("Could not post data to TSDS: " . $res_push->{'error'});
#     		return; 
#     	    }

#     	    $msg = {};
#     	    $msg->{'type'} = 'lsp';
#     	    $msg->{'node'} = $node_name;
#     	    $msg->{'lsp'} = $lsp;
#     	    $msg->{'start'} = $timestamp;
#     	    $msg->{'end'} = undef;
#     	    $msg->{'destination'} = $stats->{$lsp}->{'from'};
#     	    $msg->{'source'} = $stats->{$lsp}->{'to'};
#     	    $msg->{'path'} = $stats->{$lsp}->{'path_name'};
	    
#     	    $tmp = [];
#     	    push(@$tmp, $msg);
#     	    my $json_update = encode_json($tmp);
#     	    my $res_update = $update_svc->update_measurement_metadata(values => $json_update);
#     	    if (!defined $res_update) {
#     		log_error("Could not post metadata to TSDS: " . $res_update->{'error'});
#     		return;
#     	    }
#     	}
#     }
# }


# sub _collect {
#     my ($self) = @_;
#     log_info("Collecting...");

#     my $forker = Parallel::ForkManager->new($self->{'max_procs'});
#     $forker->run_on_finish(\&_submit_data);
    
#     foreach my $node (@{$self->{'nodes'}}) {
# 	$forker->start($node->{'name'}) and next;

# 	my $stats = $self->{'driver'}->collect_data({
# 	    node => $node,
# 						    });

# 	if (!defined $stats) {
# 	    log_error("Could not collect data on $node->{'name'}");
# 	    $forker-finish() and next;
# 	}

# 	$forker->finish(0, {stats => $stats->{'lsps'}, timestamp => $stats->{'timestamp'}});
#     }

#     $forker->wait_all_children();
# }

# sub _init {
#     my ($self) = @_;

#     log_info("Creating new config object from $self->{'config_file'}");
#     my $config = GRNOC::Config->new(
#     	config_file => $self->{'config_file'},
#     	force_array => 0
#     	);
#     $self->{'config'} = $config;

#     $interval = $config->get('/config/@interval');
#     $interval //= 10;

#     $self->{'max_procs'} = $config->get('/config/@max_procs');

#     $push_svc = GRNOC::WebService::Client->new(
# 	url => $config->get('/config/@tsds_push_service') . "/push.cgi",
# 	uid =>  $config->get('/config/@tsds_user'),
# 	passwd => $config->get('/config/@tsds_pass'),
# 	realm => $config->get('/config/@tsds_realm'),
# 	usePost => 1
# 	);

#     $update_svc = GRNOC::WebService::Client->new(
# 	url => $config->get('/config/@tsds_push_service') . "/admin.cgi",
# 	uid =>  $config->get('/config/@tsds_user'),
# 	passwd => $config->get('/config/@tsds_pass'),
# 	realm => $config->get('/config/@tsds_realm'),
# 	usePost => 1
# 	);

#     $config->{'force_array'} = 1;
#     $self->{'nodes'} = $config->get('/config/node');

#     $self->{'driver'} = GRNOC::MPLS::Collector::Driver->new();

#     $counter = GRNOC::Counter->new();
# }
# 1;

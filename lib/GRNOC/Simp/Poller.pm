package GRNOC::Simp::Poller;

use strict;
use warnings;
use Data::Dumper;
use Moo;
use Types::Standard qw( Str Bool );

use Parallel::ForkManager;
use Proc::Daemon;

use GRNOC::Config;
use GRNOC::Log;

use GRNOC::Simp::Poller::Worker;

### required attributes ###

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );

### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

### private attributes ###

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has children => ( is => 'rwp',
                  default => sub { [] } );

sub BUILD {

    my ( $self ) = @_;

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 1 );

    $self->_set_config( $config );

    return $self;
}


sub start {

    my ( $self ) = @_;

    $self->logger->info( 'Starting.' );

    $self->logger->debug( 'Setting up signal handlers.' );

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( 'Received SIG HUP.' );
    };

    # need to daemonize
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );

        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/pid-file' ) );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {

            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "simpPoller";

            $self->_create_workers();
        }
    }

    # dont need to daemonize
    else {

        $self->logger->debug( 'Running in foreground.' );

        $self->_create_workers();
    }

    return 1;
}

sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    return kill( 'TERM', @pids );
}

#-------- end of multprocess boilerplate
sub _create_workers {

    my ( $self ) = @_;



    #--- get the set of active groups 
    my $groups  = $self->config->get( "/config/group" );

    my $forker = Parallel::ForkManager->new( 10 );  #--- this really should be configurable max
    
    #--- create workers for each group
    foreach my $group (@$groups){
      next if($group->{'active'} == 0);
      my $name    = $group->{"name"};
      my $workers = $group->{'workers'};

      my $poll_interval = $group->{'poll_interval'};

      my @oids;
      foreach my$line(@{$group->{'mib'}}){
	push(@oids,$line->{'oid'});
      }

      #--- split hosts between workers
      my %hosts;
      my $idx=0;
      foreach my $host (@{$group->{'host'}}){
        push(@{$hosts{$idx}},$host);
        $idx++;
        if($idx>=$workers) { $idx = 0; }
      }

      $self->logger->info( "Creating $workers child processes for group: $name" );     

      # keep track of children pids
      $forker->run_on_start( sub {

        my ( $pid ) = @_;

        $self->logger->debug( "Child worker process $pid created." );

        push( @{$self->children}, $pid );
      } );


      # create workers
      for (my $worker_id=0; $worker_id<$workers;$worker_id++) {
        $forker->start() and next;
   
        # create worker in this process
        my $worker = GRNOC::Simp::Poller::Worker->new( worker_name => "$name-$worker_id",
						       config      => $self->config,
 						       oids        => \@oids,
						       hosts 	   => $hosts{$worker_id}, 
                                                       poll_interval => $poll_interval,
                                                       logger      => $self->logger);

        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();

        # exit child process
        $forker->finish();
      }
 
     


    } 

    $self->logger->debug( 'Waiting for all child worker processes to exit.' );

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children( [] );

    $self->logger->debug( 'All child workers have exited.' );
}

1;

package AnyEvent::Pg::Pool;

use strict;
use warnings;
use Carp qw(verbose croak);

use Data::Dumper;

use AnyEvent::Pg;

BEGIN {
    *debug = \$AnyEvent::Pg::debug;
    *_maybe_callback = \&AnyEvent::Pg::_maybe_callback;
};

our $debug;

sub _debug {
    my $pool = shift;
    my $connecting = keys %{$pool->{connecting}};
    my $idle       = keys %{$pool->{idle}};
    my $busy       = keys %{$pool->{busy}};
    local ($ENV{__DIE__}, $@);
    my ($pkg, $file, $line, $method) = (caller 1);
    $method =~ s/.*:://;
    warn "[$pool c:$connecting/i:$idle/b:$busy]\@${pkg}::$method> @_ at $file line $line\n";
}



sub new {
    my ($class, $conninfo, %opts) = @_;
    $conninfo = { %$conninfo } if ref $conninfo;
    my $size = delete $opts{size} || 1;
    my $connection_retries = delete $opts{connection_retries} || 3;
    my $connection_delay = delete $opts{connection_delay} || 2;
    my $timeout = delete $opts{timeout} || 30;
    my $on_error = delete $opts{on_error};
    my $on_connect_error = delete $opts{on_connect_error};
    # my $on_empty_queue = delete $opts{on_empty_queue};
    my $pool = { conninfo => $conninfo,
                 size => $size,
                 on_error => $on_error,
                 on_connect_error => $on_connect_error,
                 # on_empty_queue => $on_empty_queue,
                 timeout => $timeout,
                 max_conn_retries => $connection_retries,
                 conn_retries => 0,
                 conn_delay => $connection_delay,
                 conns => {},
                 current => {},
                 busy => {},
                 idle => {},
                 connecting => {},
                 queue => [],
                 seq => 1,
               };
    bless $pool, $class;
    AE::postpone { $pool->_on_start };
    $pool;
}

sub _on_start {}

sub push_query {
    my ($pool, %opts) = @_;
    my %query;
    $query{$_} = delete $opts{$_} for qw(on_result on_error on_done query args max_retries);
    my $query = \%query;
    my $queue = $pool->{queue};
    push @$queue, $query;
    $debug and $debug & 8 and $pool->_debug('query pushed into queue, raw queue size is now ' . scalar @$queue);
    AE::postpone { $pool->_check_queue };
    AnyEvent::Pg::Pool::Watcher->_new($query);
}

sub _is_queue_empty {
    my $pool = shift;
    my $queue = $pool->{queue};
    $debug and $debug & 8 and $pool->_debug('raw queue size is ' . scalar @$queue);
    while (@$queue) {
        return unless $queue->[0]{canceled};
        shift @$queue;
    }
    $debug and $debug & 8 and $pool->_debug('queue is empty');
    return 1;
}

sub _check_queue {
    my $pool = shift;
    my $idle = $pool->{idle};
    $debug and $debug & 8 and $pool->_debug('checking queue, there are '. (scalar keys %$idle) . ' idle connections');
    while (!$pool->_is_queue_empty) {
        $debug and $debug & 8 and $pool->_debug('processing first query from the query');
        unless (%$idle) {
            $debug and $debug & 8 and $pool->_debug('starting new connection');
            $pool->_start_new_conn;
            return;
        }
        my ($seq) = each %$idle;
        my $conn = $pool->{conns}{$seq}
            or croak("internal error, pool is corrupted, seq: $seq:\n" . Dumper($pool));

        delete $idle->{$seq};
        $pool->{busy}{$seq} = 1;
        my $query = shift @{$pool->{queue}};
        my $watcher = $conn->push_query(query     => $query->{query},
                                        args      => $query->{args},
                                        on_result => sub { $pool->_on_query_result($seq, @_) },
                                        on_done   => sub { $pool->_on_query_done($seq, @_) });
        $query->{watcher} = $watcher;
        $pool->{current}{$seq} = $query;
        $debug and $debug & 8 and $pool->_debug("query $query started on conn $conn, seq: $seq");
    }
    $debug and $debug & 8 and $pool->_debug('queue is empty!');
}

sub _on_query_result {
    my ($pool, $seq, $conn, $result) = @_;
    my $query = $pool->{current}{$seq};
    if ($debug and $debug & 8) {
        $pool->_debug("query result $result received for query $query on connection $conn, seq: $seq");
        $result->status == Pg::PQ::PGRES_FATAL_ERROR and
            $pool->_debug("errorDescription:\n" . Dumper [$result->errorDescription]);
    }
    if ($query->{fatal_error_seen}) {
        $debug and $debug & 8 and $pool->_debug("fatal_error_seen is set, ignoring later on_result");
    }
    else {
        if ($result->status == Pg::PQ::PGRES_FATAL_ERROR and $result->errorField('severity') ne 'ERROR') {
            $pool->_debug("this is a real FATAL error, skipping the on_result callback");
            $query->{fatal_error_seen}++;
        }
        else {
            $query->{max_retries} = 0;
            $pool->_maybe_callback($query, 'on_result', $conn, $result);
        }
    }
}

sub _on_query_done {
    my ($pool, $seq, $conn) = @_;
    my $query = delete $pool->{current}{$seq};
    $pool->_maybe_callback($query, ($query->{fatal_error_seen} ? 'on_error' : 'on_done'), $conn);
}

sub _start_new_conn {
    my $pool = shift;
    if (keys %{$pool->{conns}} < $pool->{size}            and
        $pool->{conn_retries} < $pool->{max_conn_retries} and
        !%{$pool->{connecting}}                           and
        !$pool->{delay_watcher}) {
        my $seq = $pool->{seq}++;
        my $conn = AnyEvent::Pg->new($pool->{conninfo},
                                     timeout => $pool->{timeout},
                                     on_connect => sub { $pool->_on_conn_connect($seq, @_) },
                                     on_connect_error => sub { $pool->_on_conn_connect_error($seq, @_) },
                                     on_empty_queue => sub { $pool->_on_conn_empty_queue($seq, @_) },
                                     on_error => sub { $pool->_on_conn_error($seq, @_) },
                                    );
        $debug and $debug & 8 and $pool->_debug("new connection started, seq: $seq, conn: $conn");
        $pool->{conns}{$seq} = $conn;
        $pool->{connecting}{$seq} = 1;
    }
    else {
        $debug and $debug & 8 and $pool->_debug('not starting new connection, conns: ' . (scalar keys %{$pool->{conns}}) .
                                                ", retries: $pool->{conn_retries}, connecting: ".(scalar keys %{$pool->{connecting}}) .
                                                ", delay watcher: $pool->{delay_watcher}");
    }
}

sub _on_conn_error {
    my ($pool, $seq, $conn) = @_;

    if (my $query = delete $pool->{current}{$seq}) {
        if ($query->{max_retries}-- > 0) {
            undef $query->{watcher};
            unshift @{$pool->{queue}}, $query;
        }
        else {
            $pool->_maybe_callback($query, 'on_error', $conn);
        }
    }
    if ($debug and $debug & 8) {
        my @states = grep $pool->{$_}{$seq}, qw(busy idle connecting);
        $pool->_debug("removing broken connection in state(s!) @states, "
                      . "\$conn: $conn, \$pool->{conns}{$seq}: "
                      . ($pool->{conns}{$seq} // '<undef>'));
    }
    delete $pool->{busy}{$seq}
        or croak "internal error, pool is corrupted, seq: $seq\n" . Dumper($pool);
    delete $pool->{conns}{$seq};
    $pool->_check_queue;
}

sub _on_conn_connect {
    my ($pool, $seq, $conn) = @_;
    $debug and $debug & 8 and $pool->_debug("conn $conn is now connected, seq: $seq");
    $pool->{conn_retries} = 0;
    # _on_conn_empty_queue is called afterwards by the $conn object
}

sub _on_conn_connect_error {
    my ($pool, $seq, $conn) = @_;
    $debug and $debug & 8 and $pool->_debug("unable to connect to database");
    # the connection object will be removed from the Pool on the
    # on_error callback that will be called just after this one
    # returns:
    delete $pool->{connecting}{$seq};
    $pool->{busy}{$seq} = 1;
    if ($pool->_is_queue_empty) {
        $pool->{conn_retries} = 0;
    }
    else {
        if ($pool->{conn_retries}++ < $pool->{max_conn_retries}) {
            $debug and $debug & 8 and $pool->_debug("starting timer for delayed reconnection");
            $pool->{delay_watcher} = AE::timer $pool->{conn_delay}, 0, sub { $pool->_on_delayed_reconnect };
        }
        else {
            $pool->_maybe_callback('on_connect_error', $conn);
        }
    }
}

sub _on_delayed_reconnect {
    my $pool = shift;
    $debug and $debug & 8 and $pool->_debug("_on_delayed_reconnect called");
    undef $pool->{delay_watcher};
    $pool->_start_new_conn;
}

sub _on_conn_empty_queue {
    my ($pool, $seq, $conn) = @_;
    $debug and $debug & 8 and $pool->_debug("conn $conn queue is now empty, seq: $seq");

    unless (delete $pool->{busy}{$seq} or
            delete $pool->{connecting}{$seq}) {
        if ($debug) {
            $pool->_debug("pool object: \n" . Dumper($pool));
            croak "internal error: empty_queue callback invoked by object not in state busy or connecting, seq: $seq";
        }
    }
    $pool->{idle}{$seq} = 1;
    $pool->_check_queue;
}

package AnyEvent::Pg::Pool::Watcher;

sub _new {
    my ($class, $query) = @_;
    my $watcher = \$query;
    bless $watcher, $class;
}

sub DESTROY {
    my $query = ${shift()};
    delete @{$query}{qw(watcher)};
    $query->{canceled} = 1;
}

1;

package AnyEvent::Pg::Pool;

use strict;
use warnings;
use Carp;

use AnyEvent::Pg;

BEGIN {
    *debug = \$AnyEvent::Pg::debug;
    *_debug = \&AnyEvent::Pg::_debug;
    *_maybe_callback = \&AnyEvent::Pg::_maybe_callback;
};

our $debug;

sub new {
    my ($class, $conninfo, %opts) = @_;
    $conninfo = { %$conninfo } if ref $conninfo;
    my $size = delete $opts{size} || 1;
    my $connection_retries = delete $opts{connection_retries} || 3;
    my $connection_delay = delete $opts{connection_delay} || 0;
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
    @$queue or AE::postpone { $pool->_check_queue };
    push @{$pool->{queue}}, $query;
    AnyEvent::Pg::Pool::Watcher->_new($query);
}

sub _is_queue_empty {
    my $pool = shift;
    my $queue = $pool->{queue};
    while (@$queue) {
        return 1 unless $queue->[0]{canceled};
        shift @$queue;
    }
    return undef;
}

sub _check_queue {
    my $pool = shift;
    my $idle = $pool->{idle};
    while (!$pool->_is_queue_empty) {
        unless (%$idle) {
            $pool->_start_new_conn;
            return;
        }
        my ($seq) = each %$idle;
        delete $idle->{$seq};
        $pool->{busy}{$seq} = 1;
        my $conn = $pool->{conns}{$seq};
        my $query = shift @{$pool->{queue}};
        my $watcher = $conn->push_query(query     => $query->{query},
                                        args      => $query->{args},
                                        on_result => sub { $pool->_on_query_result($seq, @_) },
                                        on_done   => sub { $pool->_on_query_done($seq, @_) });
        $query->{watcher} = $watcher;
    }
}

sub _on_query_result {
    my ($pool, $seq, $conn, $result);
    my $query = $pool->{current}{$seq};
    $query->{max_retries} = 0;
    $pool->_maybe_callback($query, 'on_result', $conn, $result);
}

sub _on_query_done {
    my ($pool, $seq, $conn) = @_;
    my $query = delete $pool->{current}{$seq};
    $pool->_maybe_callback($query, 'on_done', $conn);
}

sub _start_new_conn {
    my $pool = shift;
    if (keys %{$pool->{conns}} < $pool->{max}             and
        $pool->{conn_retries} < $pool->{max_conn_retries} and
        %{$pool->{connecting}}                            and
        !$pool->{delay_watcher}) {
        my $seq = $pool->{seq}++;
        my $conn = AnyEvent::Pg->new($pool->{conninfo},
                                     timeout => $pool->{timeout},
                                     on_connect => sub { $pool->_on_conn_connect($seq) },
                                     on_connect_error => sub { $pool->_on_conn_connect_error($seq) },
                                     on_empty_queue => sub { $pool->_on_conn_empty_queue($seq) },
                                     on_error => sub { $pool->_on_conn_error($seq) },
                                    );
        $pool->{conns}{$seq} = $conn;
        $pool->{connecting}{$seq} = 1;
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
    delete $pool->{busy}{$seq};
    delete $pool->{conns}{$seq};
    $pool->_check_queue;
}

sub _on_conn_connect {
    my ($pool, $seq) = @_;
    $pool->{conn_retries} = 0;
    # _on_conn_empty_queue is called afterwards by the $conn object
}

sub _on_conn_connect_error {
    my ($pool, $conn, $seq) = @_;
    delete $pool->{conns}{$seq};
    delete $pool->{connecting}{$seq};
    if ($pool->_is_queue_empty) {
        $pool->{conn_retries} = 0;
    }
    else {
        if ($pool->{conn_retries}++ < $pool->{max_conn_retries}) {
            $pool->{delay_watcher} = AE::timer $pool->{conn_delay}, 0, sub { $pool->_on_delayed_reconnect };
        }
        else {
            $pool->_maybe_callback('on_connect_error', $conn);
        }
    }
}

sub _on_delayed_reconnect {
    my $pool = shift;
    undef $pool->{delay_watcher};
    $pool->_start_new_conn;
}

sub _on_conn_empty_queue {
    my ($pool, undef, $seq) = @_;
    delete $pool->{busy}{$seq} or
        delete $pool->{connecting}{$seq} or
            croak "internal error: empty_queue callback invoked by object not in state busy or connecting";
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

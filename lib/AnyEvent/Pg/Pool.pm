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
    my $delayed    = ($pool->{delay_watcher} ? 1 : 0);
    my $total      = keys %{$pool->{conns}};
    local ($ENV{__DIE__}, $@);
    my ($pkg, $file, $line, $method) = (caller 1);
    $method =~ s/.*:://;
    warn "[$pool c:$connecting/i:$idle/b:$busy|t:$total|d:$delayed]\@${pkg}::$method> @_ at $file line $line\n";
}



sub new {
    my ($class, $conninfo, %opts) = @_;
    $conninfo = { %$conninfo } if ref $conninfo;
    my $size = delete $opts{size} || 1;
    my $connection_retries = delete $opts{connection_retries} || 3;
    my $connection_delay = delete $opts{connection_delay} || 2;
    my $timeout = delete $opts{timeout} || 30;
    my $scavenge_timeout = delete $opts{scavenge_timeout} || 0;
    my $scavenge_interval = delete $opts{scavenge_interval} || 1;
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
                 scavenge_timeout => $scavenge_timeout,
                 scavenge_interval => $scavenge_interval,
                 scavenge_watcher => undef,
                 conns => {},
                 current => {},
                 busy => {},
                 idle => {},
                 connecting => {},
                 queue => [],
                 seq => 1,
                 query_seq => 1,
               };
    bless $pool, $class;
    AE::postpone { $pool->_on_start };
    $pool;
}

sub is_dead { shift->{dead} }

sub _on_start {
    my $pool = shift;
    if( ($pool->{scavenge_timeout} > 0) and ($pool->{scavenge_interval} > 0) ) {
        $pool->{scavenge_watcher} = AE::timer $pool->{scavenge_interval}, $pool->{scavenge_interval}, sub { $pool->_scavenge };
    };
}

sub _scavenge {
    my $pool = shift;
    my $idle = $pool->{idle};
    if( %$idle ) {
        my $now = AE::now;
        my $scavenge_timeout = $pool->{scavenge_timeout};
        for my $seq (grep { $now - $idle->{$_} > $scavenge_timeout } keys %$idle) {
            (delete $pool->{idle}{$seq} and $pool->{conns}{$seq}->destroy and delete $pool->{conns}{$seq})
                or croak "internal error, pool is corrupted, seq: $seq\n" . Dumper($pool);
        }
    }
}

sub push_query {
    my ($pool, %opts) = @_;
    my %query;
    my $retry_on_sqlstate = delete $opts{retry_on_sqlstate};
    $retry_on_sqlstate = { map { $_ => 1 } @$retry_on_sqlstate }
        if ref($retry_on_sqlstate) eq 'ARRAY';
    $query{retry_on_sqlstate} = $retry_on_sqlstate // {};
    $query{$_} = delete $opts{$_} for qw(on_result on_error on_done query args max_retries);
    $query{seq} = $pool->{query_seq}++;
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
            if ($pool->{dead}) {
                my $query = shift @{$pool->{queue}};
                $pool->_maybe_callback($query, 'on_error');
                next;
            }
            $debug and $debug & 8 and $pool->_debug('starting new connection');
            $pool->_start_new_conn;
            return;
        }
        my $seq = undef;
        (not defined $seq or $seq > $_) and $seq = $_ for keys %$idle;
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

my %error_severiry_fatal = map { $_ => 1 } qw(FATAL PANIC);

sub _on_query_result {
    my ($pool, $seq, $conn, $result) = @_;
    my $query = $pool->{current}{$seq};
    if ($debug and $debug & 8) {
        $pool->_debug("query result $result received for query $query on connection $conn, seq: $seq");
        $result->status == Pg::PQ::PGRES_FATAL_ERROR and
            $pool->_debug("errorDescription:\n" . Dumper [$result->errorDescription]);
    }
    if ($query->{retry}) {
        $debug and $debug & 8 and $pool->_debug("retry is set, ignoring later on_result");
    }
    else {
        if ($query->{max_retries} and $result->status == Pg::PQ::PGRES_FATAL_ERROR) {
            if ($query->{retry_on_sqlstate}{$result->errorField('sqlstate')}) {
                $pool->_debug("this is a retry-able error, skipping the on_result callback");
                $query->{retry} = 1;
                return;
            }
            if ($error_severiry_fatal{$result->errorField('severity')}) {
                $pool->_debug("this is a real FATAL error, skipping the on_result callback");
                $query->{retry} = 1;
                return;
            }
        }
        $query->{max_retries} = 0;
        $pool->_maybe_callback($query, 'on_result', $conn, $result);
    }
}

sub _on_query_done {
    my ($pool, $seq, $conn) = @_;
    my $query = delete $pool->{current}{$seq};
    if (delete $query->{retry}) {
        $debug and $debug & 8 and $pool->_debug("unshifting failed query into queue");
        $query->{max_retries}--;
        unshift @{$pool->{queue}}, $query;
    }
    else {
        $pool->_maybe_callback($query, 'on_done', $conn);
    }
}

sub _start_new_conn {
    my $pool = shift;
    if (keys %{$pool->{conns}} < $pool->{size}             and
        !%{$pool->{connecting}}                            and
        $pool->{conn_retries} <= $pool->{max_conn_retries} and
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
        $debug and $debug & 8 and $pool->_debug('not starting new connection, conns: '
                                                . (scalar keys %{$pool->{conns}})
                                                . ", retries: $pool->{conn_retries}, connecting: "
                                                . (scalar keys %{$pool->{connecting}}));
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
    delete $pool->{busy}{$seq} or delete $pool->{idle}{$seq}
        or croak "internal error, pool is corrupted, seq: $seq\n" . Dumper($pool);
    delete $pool->{conns}{$seq};
    $pool->_maybe_callback('on_connect_error', $conn) if $pool->{dead};
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

    if ($pool->{delay_watcher}) {
        $debug and $debug & 8 and $pool->_debug("a delayed reconnection is already queued");
    }
    else {
        # This failed connection is not counted against the limit
        # unless it is the only connection remaining. Effectively the
        # module will keep going until all the connections become
        # broken and no more connections can be established.
        $pool->{conn_retries}++ unless keys(%{$pool->{conns}}) > 1;

        if ($pool->{conn_retries} <= $pool->{max_conn_retries}) {
            $debug and $debug & 8 and $pool->_debug("starting timer for delayed reconnection $pool->{conn_delay}s");
            $pool->{delay_watcher} = AE::timer $pool->{conn_delay}, 0, sub { $pool->_on_delayed_reconnect };
        }
        else {
            # giving up!
            $debug and $debug & 8 and $pool->_debug("it has been imposible to connect to the database, giving up!!!");
            $pool->{dead} = 1;
            # processing continues on the on_conn_error callback
        }
    }
}

sub _on_fatal_connect_error {
    my ($pool, $conn) = @_;
    # This error is fatal. After it happens, everything is going to
    # fail.
    $pool->{dead} = 1;

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
    $pool->{idle}{$seq} = AE::now;
    $pool->_check_queue;
}

sub destroy {
    my $pool = shift;
    my $conns = $pool->{conns};
    $conns->{$_}->destroy for keys %$conns;
    %$pool = ();
    1;
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

=head1 NAME

AnyEvent::Pg::Pool

=head1 SYNOPSIS

  my $pool = AnyEvent::Pg::Pool->new($conninfo,
                                     on_connect_error => \&on_db_is_dead);

  $pool->push_query(query => 'select * from foo',
                    on_result => sub { ... });

=head1 DESCRIPTION

  *******************************************************************
  ***                                                             ***
  *** NOTE: This is a very early release that may contain lots of ***
  *** bugs. The API is not stable and may change between releases ***
  ***                                                             ***
  *******************************************************************

This module handles a pool of databases connections, and transparently
handles reconnection and reposting queries on case of network or
server errors.

=head2 API

The following methods are provided:

=over 4

=item $pool = AnyEvent::Pg::Pool->new($conninfo, %opts)

Creates a new object.

Accepts the following options:

=over 4

=item size => $size

Maximun number of database connections that can be simultaneously
established with the server.

=item connection_retries => $n

Maximum number of attempts to establish a new database connection
before calling the C<on_connect_error> callback when there is no other
connection alive on the pool.

=item connection_delay => $seconds

When establishing a new connection fails, this setting allows to
configure the number of seconds to delay before trying to connect
again.

=item timeout => $seconds

When some active connection does not report activity for the given
number of seconds, it is considered dead and closed.

=item on_error => $callback

When some error happens that can not be automatically handled by the
module (for instance, by requeuing the current query), this callback
is invoked.

=item on_connect_error => $callback

When the number of failed reconnection attemps goes over the limit,
this callback is called. The pool object and the L<AnyEvent::Pg>
object representing the last failed attempt are passed as arguments.

=back

=item $w = $pool->push_query(%opts)

Pushes a database query on the pool queue. It will be sent to the
database once any of the database connections becomes idle.

A watcher object is returned. If that watcher goes out of scope, the
query is canceled.

This method accepts all the options supported by the method of the
same name on L<AnyEvent::Pg> plus the following ones:

=over 4

=item retry_on_sqlstate => \@states

=item retry_on_sqlstate => \%states

A hash of sqlstate values that are retryable. When some error happens,
and the value of sqlstate from the result object has a value on this
hash, the query is reset and reintroduced on the query.

=item max_retries => $n

Maximum number of times a query can be retried. When this limit is
reached, the on_error callback will be called.

Note that queries are not retried after partial success. For instance,
when a result object is returned, but then the server decides to abort
the transaction (this is rare, but can happen from time to time).

=back

=back

=head1 SEE ALSO

L<AnyEvent::Pg>, L<Pg::PQ>, L<AnyEvent>.

=head1 BUGS AND SUPPORT

This is a very early release that may contain lots of bugs.

Send bug reports by email or using the CPAN bug tracker at
L<https://rt.cpan.org/Dist/Display.html?Status=Active&Queue=AnyEvent-Pg>.

=head2 Commercial support

This module was implemented during the development of QVD
(L<http://theqvd.com>) the Linux VDI platform.

Commercial support, professional services and custom software
development services around this module are available from QindelGroup
(L<http://qindel.com>). Send us an email with a rough description of your
requirements and we will get back to you ASAP.

=head1 AUTHOR

Salvador FandiE<ntilde>o, E<lt>sfandino@yahoo.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Qindel FormaciE<oacute>n y Servicios S.L.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.

=cut

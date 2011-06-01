package AnyEvent::Pg;

our $VERSION = 0.01;

use 5.010;
use strict;
use warnings;
use Carp;

use AnyEvent;
use Pg::PQ qw(:pgres_polling);

sub _check_state {
    my $self = shift;
    my $state = $self->{state};
    for (@_) {
        return if $_ eq $state;
    }
    my $upsub = (caller 1)[3];
    croak "$upsub can not be called in state $state";
}

sub _ensure_list { ref $_[0] ? @{$_[0]} : $_[0] }


sub new {
    my ($class, $conninfo, %opts) = @_;

    my $on_connect = delete $opts{on_connect};
    my $on_connect_error = delete $opts{on_connect_error};
    my $on_empty_queue = delete $opts{on_empty_queue};

    my $dbc = Pg::PQ::Conn->start($conninfo);
    # FIXME: $dbc may be undef
    my $self = { state => 'connecting',
                 dbc => $dbc,
                 on_connect => $on_connect,
                 on_connect_error => $on_connect_error,
                 queries => [],
               };
    bless $self, $class;

    $self->_connectPoll;

    $self;
}

sub dbc { shift->{dbc} }

sub _connectPoll {
    my $self = shift;
    my $dbc = $self->{dbc};
    my $fd = $self->{fd};

    say "_connectPoll";

    my $r;
    if (defined $fd) {
        $r = $dbc->connectPoll;
    }
    else {
        $fd = $self->{fd} = $dbc->socket;
        $r = PGRES_POLLING_WRITING;
    }

    say $r;

    my $goto;
    my $rw;
    my $ww;

    given ($r) {
        when (PGRES_POLLING_READING) {
            $rw = $self->{read_watcher} // AE::io $fd, 0, sub { $self->_connectPoll };
            say "fd: $fd, read_watcher: $rw";
        }
        when (PGRES_POLLING_WRITING) {
            $ww = $self->{write_watcher} // AE::io $fd, 1, sub { say "can write"; $self->_connectPoll };
            say "fd: $fd, write_watcher: $ww";
        }
        when (PGRES_POLLING_FAILED) {
            $goto = '_on_connect_error';
        }
        when (PGRES_POLLING_OK) {
            $goto = '_on_connect';
        }
    }
    $self->{read_watcher} = $rw;
    $self->{write_watcher} = $ww;
    no warnings 'uninitialized';
    say "read_watcher: $rw, write_watcher: $ww";

    $self->$goto if $goto;
}

sub _call_back {
    my $cb = shift;
    $cb->(@_) if $cb;
}

sub _on_connect {
    my $self = shift;
    my $dbc = $self->{dbc};
    $dbc->nonBlocking(1);
    $self->{state} = 'connected';
    _call_back($self->{on_connect}, $self);
    $self->_on_push_query;
}

sub _on_connect_error {
    my $self = shift;
    $self->{state} = 'failed';
}

sub push_query {
    my $self = shift;
    $self->_check_state('connecting', 'connected');
    push @{$self->{queries}}, { @_ };
    $self->_on_push_query if $self->{state} eq 'connected';
}

sub _on_push_query {
    my $self = shift;
    unless ($self->{current_query}) {
        my $queries = $self->{queries};
        if (@$queries) {
            $self->{write_watcher} = AE::io $self->{fd}, 1, sub { $self->_on_push_query_writable };
        }
        else {
            _call_back($self->{on_empty_queue}, $self);
        }
    }
}

sub _on_push_query_writable {
    my $self = shift;
    delete $self->{write_watcher};

    my $dbc = $self->{dbc};
    my $query = shift @{$self->{queries}};
    my $r = $dbc->sendQuery(_ensure_list $query->{query});
    if ($r) {
        $self->{current_query} = $query;
        $self->_on_consume_input;
    }
    else {
        _call_back($query->{on_error}, $self);
        $self->_on_push_query
    }
}

sub _on_consume_input {
    my $self = shift;
    my $dbc = $self->{dbc};
    $dbc->consumeInput;
    if ($dbc->busy) {
        $self->{consume_input_watcher} //= AE::io $self->{fd}, 0, sub { $self->_on_consume_input }
    }
    else {
        delete $self->{consume_input_watcher};
        my $result = $dbc->result;
        _call_back($self->{current_query}{on_result}, $self, $result);
        undef $self->{current_query};
        $self->_on_push_query;
    }
}

1;
__END__

=head1 NAME

AnyEvent::Pg - Query a PostgreSQL database asynchronously

=head1 SYNOPSIS

  use AnyEvent::Pg;
  my $db = AnyEvent::Pg->new("dbname=foo",
                             on_connect => sub { ... });

=head1 DESCRIPTION

Stub documentation for AnyEvent::Pg, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Salvador Fandino, E<lt>salva@E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Salvador Fandino

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut

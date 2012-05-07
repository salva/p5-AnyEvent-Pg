#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 3;

use FindBin;
use lib "$FindBin::Bin/../lib";
use AnyEvent::Pg;

my @queries;
my $cv = AE::cv;
my $notifs = {};
my $db;

SKIP: {
    skip "Set \$ENV{PG_NOTIFY_TESTS} to run listen-notify tests", 3 unless $ENV{PG_NOTIFY_TESTS};

    run_tests();
}

sub run_tests {
    $db = AnyEvent::Pg->new(
        '',
        on_connect => \&connected,
        on_notify => \&got_notification,
        on_error => \&error,
        timeout => 3,
    );

    $cv->recv;
    check_notifs();
}

###

# check to see if we got all expected notifications
sub check_notifs {
    is($notifs->{a}, 2, "Got expected notif callbacks");
    is($notifs->{b}, undef, "Got expected notif callbacks");
}

sub error {
    warn "Error: @_";
    $cv->send;
}

sub got_notification {
    my ($pq, $channel, $pid) = @_;
    $notifs->{$channel}++;
    $cv->end;
}

sub connected {
    push @queries, $db->listen('a' => (
        on_error => sub {
            my ($pq, $err) = @_;
            fail("Error listening: $err");
            $cv->send;
        },
        on_result => sub {
            my ($pq, $res) = @_;
            pass("Got listen query success");
        },
    ));

    # should get notified
    $cv->begin;
    push @queries, $db->notify('a' => 'abc123*!(%&(!$#@":');

    # should not get notified
    $cv->begin;
    push @queries, $db->notify('b');
    $cv->end;

    $cv->begin;
    {
        # should get notified again
        $cv->begin;
        push @queries, $db->notify('a');

        # unsubscribe from a
        push @queries, $db->unlisten('a');
    
        # should not get notified
        $cv->begin;
        push @queries, $db->notify('a');
        $cv->end;
    }
    $cv->end;
}


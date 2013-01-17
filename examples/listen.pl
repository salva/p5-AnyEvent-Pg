#!/usr/bin/perl

use strict;
use warnings;
use 5.010;

use AE;
use AnyEvent::Pg::Pool;

$Pg::PQ::debug = -1;
$AnyEvent::Pg::debug = -1;

my $cv = AE::cv();

my $db = AnyEvent::Pg::Pool->new("dbname=pgpqtest");
my $w = $db->listen('foo',
                    on_listener_started => sub { say "started!!!" },
                    on_notify           => sub { say "foo!" });

#my $qw;
#my $tw = AE::timer 10, 10, sub { $qw = $db->push_query(query => "select now()") };

warn "waiting for notification!\n";


$cv->recv();

print "watcher: $w\n";

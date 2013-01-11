#!/usr/bin/perl

use strict;
use warnings;
use 5.010;

use AE;
use AnyEvent::Pg::Pool;

my $db = AnyEvent::Pg::Pool->new("dbname=pgpqtest");
my $w = $db->listen('foo', on_notify => sub { say "foo!" });

my $cv = AE::cv();
$cv->recv();

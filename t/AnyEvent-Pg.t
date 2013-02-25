#!/usr/bin/perl

use strict;
use warnings;
use 5.010;

# use Devel::FindRef;

$| = 1;
use Pg::PQ qw(:pgres);
use AnyEvent::Pg;
use Test::More;

my ($ci, $tpg);

if (defined $ENV{TEST_ANYEVENT_PG_CONNINFO}) {
    $ci = $ENV{TEST_ANYEVENT_PG_CONNINFO};
}
else {
    unless (eval { require Test::postgresql; 1 }) {
        plan skip_all => "Unable to load Test::postgresql: $@";
    }

    $tpg = Test::postgresql->new;
    unless ($tpg) {
        no warnings;
        plan skip_all => "Test::postgresql failed to provide a database instance: $Test::postgresql::errstr";
    }


    $ci = { dbname => 'test',
            host   => '127.0.0.1',
            port   => $tpg->port,
            user   => 'postgres' };

    # use Data::Dumper;
    # diag(Data::Dumper->Dump( [$tpg, $ci], [qw(tpg *ci)]));
}

my @w;
my $queued = 0;

sub ok_query {
    my ($pg, @query) = @_;
    $queued++;
    my $ok;
    push @w, $pg->push_query(query => \@query,
                             on_error => sub {
                                 fail("query '@query' error: " . $_[1]->error);
                                 $queued--;
                             },
                             on_done  => sub {
                                 ok($ok, "query '@query'");
                                 $queued--;
                             },
                             on_result => sub {
                                 my $status = $_[1]->status;
                                 $ok = 1 if $status == PGRES_TUPLES_OK or $status == PGRES_COMMAND_OK;
                             } );
}

sub fail_query {
    my ($pg, @query) = @_;
    $queued++;
    my $ok;
    push @w, $pg->push_query(query => \@query,
                             on_error => sub {
                                 fail("query '@query' error: " . $_[1]->error);
                                 $queued--;
                             },
                             on_done  => sub {
                                 ok(!$ok, "query '@query' should fail");
                                 $queued--;
                             },
                             on_result => sub {
                                 my $status = $_[1]->status;
                                 $ok = 1 if $status == PGRES_TUPLES_OK or $status == PGRES_COMMAND_OK;
                             } );
}


sub ok_query_prepare {
    my ($pg, $name, $query) = @_;
    $queued++;
    my $ok;
    push @w, $pg->push_prepare(name => $name, query => $query,
                               on_error => sub {
                                   fail("prepare query $name => '$query' error: " . $_[1]->error);
                                   $queued--;
                               },
                               on_done  => sub {
                                   ok($ok, "prepare query $name => '$query' passed");
                                   $queued--;
                               },
                               on_result => sub {
                                   my $status = $_[1]->status;
                                   $ok = 1 if $status == PGRES_COMMAND_OK;
                               } );
}

sub ok_query_prepared {
    my ($pg, $name, @args) = @_;
    $queued++;
    my $ok;
    push @w, $pg->push_query_prepared(name => $name, args => \@args,
                                      on_error => sub {
                                          fail("prepared query $name => '@args' error: " . $_[1]->error);
                                          $queued--;
                                      },
                                      on_done  => sub {
                                          ok($ok, "prepared query $name => '@args' passed");
                                          $queued--;
                                      },
                                      on_result => sub {
                                          my $status = $_[1]->status;
                                          $ok = 1 if $status == PGRES_TUPLES_OK or $status == PGRES_COMMAND_OK;
                                      } );

}

###########################################################################################33
#
# Tests go here:
#
#


plan tests => 18;
diag "conninfo: " . Pg::PQ::Conn::_make_conninfo($ci);

my $timer;
my $cv = AnyEvent->condvar;
my $pg = AnyEvent::Pg->new($ci,
                           on_connect       => sub { pass("connected") },
                           on_connect_error => sub { fail("connect error") },
                           on_empty_queue   => sub {
                               ok ($queued == 0, "queue is empty");
                               undef $timer;
                               $cv->send;
                           } );

fail_query($pg, 'drop table foo');
fail_query($pg, 'drop table bar');
ok_query($pg, 'create table foo (id int, name varchar(20))');
ok_query_prepare($pg, populate_foo => 'insert into foo (id, name) values ($1, $2)');

my %data = ( hello => 10, hola => 45, cheers => 1);
ok_query($pg, 'insert into foo (id, name) values ($1, $2)', $data{$_}, $_)
    for keys %data;

ok_query_prepare($pg, foo_bigger => 'select * from foo where id > $1 order by id desc');

my %data1 = ( bye => 12, goodbye => 13, adios => 111, 'hasta la vista' => 41);
ok_query_prepared($pg, populate_foo => $data1{$_}, $_)
    for keys %data1;

ok_query($pg, 'select * from foo');
ok_query_prepared($pg, 'foo_bigger', 12);
ok_query($pg, 'select * from foo where id < 12 order by name; select * from foo where id > 12 order by name');

$timer = AE::timer 120, 0, sub {
    fail("timeout");
    $cv->send;
};

$cv->recv;
pass("after recv");

# print Devel::FindRef::track(\$pg), "\n";
undef $pg;
undef @w;











# -*- mode:perl -*-
use strict;
use Test::More qw/ no_plan /;
use Test::Exception;
use Benchmark qw/ :all /;
BEGIN { use_ok 'DBIx::CouchLike' }

my $dbh = require 't/connect.pl';
ok $dbh;

my $couch = DBIx::CouchLike->new({ dbh => $dbh, table => "foo" });
ok $couch->create_table;

my $n = 0;
my @id;

select STDERR;
timethese( 2000, {
    post => sub {
        push @id, $couch->post({ number => ++$n });
    },
    put => sub {
        ++$n;
        $couch->put( "put_$n" => { number => $n } );
    },
});
$n = 0;
timethese( 1000, {
    get => sub {
        $couch->get( $id[ $n++ ] );
    },
    delete => sub {
        ++$n;
        $couch->delete("put_$n");
    },
});

my $map = q|
sub {
    my ($doc, $emit) = @_;
    no warnings;
    my $tail = substr $doc->{_id}, -2;
    $emit->($tail, 1);
}|;

my $reduce = q|
sub {
    my ($keys, $values) = @_;
    return List::Util::sum(@$values);
}|;

use List::Util qw/ sum /;
my $design = {
    _id      => "_design/count",
    views    => {
        by_tail_of_id => {
            map    => $map,
        },
    },
};
$couch->post($design);

timethese( 0, {
    map => sub {
        my $r = $couch->view('count/by_tail_of_id');
    },
    map_key => sub {
        my $r = $couch->view('count/by_tail_of_id', { key => "12" });
    },
    map_key_like => sub {
        my $r = $couch->view('count/by_tail_of_id', { key_like => "12%" });
    },
    map_key_sw => sub {
        my $r = $couch->view('count/by_tail_of_id', { key_start_with => "12" });
    },
    map_key_docs => sub {
        my $r = $couch->view('count/by_tail_of_id', { key => "12", include_docs => 1, });
    },
});

$design->{views}->{by_tail_of_id}->{reduce} = $reduce;
$couch->put($design);

timethese( 0, {
    map_reduce => sub {
        my $r = $couch->view('count/by_tail_of_id');
    },
    map_key_reduce => sub {
        my $r = $couch->view('count/by_tail_of_id', { key => "12" });
    },
});

$dbh->commit unless $ENV{DSN};
$dbh->disconnect;

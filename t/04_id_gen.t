# -*- mode:perl -*-
use strict;
use Test::More qw/ no_plan /;
use Test::Exception;
BEGIN { use_ok 'DBIx::CouchLike::IdGenerator' }

my $gen = DBIx::CouchLike::IdGenerator->new;
isa_ok $gen => "DBIx::CouchLike::IdGenerator";
ok $gen->can('get_id');

my %id;
for ( 1 .. 10000 ) {
    my $new = $gen->get_id;
    ok !$id{$new};
    $id{$new} = 1;
}

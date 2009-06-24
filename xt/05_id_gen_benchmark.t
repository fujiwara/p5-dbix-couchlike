# -*- mode:perl -*-
use strict;
use Test::More qw/ no_plan /;
use Test::Exception;
use Benchmark qw/ :all /;
BEGIN {
    use_ok 'DBIx::CouchLike::IdGenerator';
    eval { require Data::YUID::Generator };
    if ($@) {
        exit
    }
}

my $yuid  = Data::YUID::Generator->new;
my $couch = DBIx::CouchLike::IdGenerator->new;

select STDERR;

diag "Benchmarking get_id";
cmpthese timethese( 0, {
    "Data::YUID" => sub {
        $yuid->get_id;
    },
    "DBIx::CouchLike" => sub {
        $couch->get_id;
    },
});

diag "Benchmarking new->get_id";
cmpthese timethese( 0, {
    "Data::YUID" => sub {
        Data::YUID::Generator->new->get_id;
    },
    "DBIx::CouchLike" => sub {
        DBIx::CouchLike::IdGenerator->new->get_id;
    },
});


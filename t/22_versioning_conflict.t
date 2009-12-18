# -*- mode:perl -*-
use strict;
use Test::More;
use Test::Exception;
BEGIN { use_ok 'DBIx::CouchLike' }

my $dbh = require 't/connect.pl';
ok $dbh;

my $couch = DBIx::CouchLike->new({ dbh => $dbh, table => "foo", versioning => 1 });
isa_ok $couch => "DBIx::CouchLike";
ok $couch->can('dbh');
is $couch->dbh => $dbh;
ok $couch->dbh->ping;
ok $couch->versioning;

is $couch->table => "foo";
ok $couch->create_table;

my $id = $couch->post({ foo => 1, bar => 2 });
ok $id;

my $obj = $couch->get($id);
ok $obj;
is_deeply $obj => { foo => 1, bar => 2, _id => $id, _version => 0 };

ok $couch->put({ %$obj, baz => 3 });
throws_ok { $couch->put($obj) } qr/Can't put/;

$obj = $couch->get($id);
is_deeply $obj => { foo => 1, bar => 2, baz => 3, _id => $id, _version => 1 };

ok $couch->put($obj);

$dbh->commit unless $ENV{DSN};
$dbh->disconnect;

done_testing;

# -*- mode:perl -*-
use strict;
use Test::More qw/ no_plan /;
use Test::Exception;
BEGIN { use_ok 'DBIx::CouchLike' }

my $dbh = require 't/connect.pl';
ok $dbh;
do_sql($dbh);

my $couch_p = DBIx::CouchLike->new({ dbh => $dbh, table => "page" });
my @v = $couch_p->view('all/list');
is_deeply( \@v => [
    { 'value' => '1', 'id' => '/default', 'key' => '/default' },
]);

my $p = $couch_p->get('/default');
$couch_p->put($p);

@v = $couch_p->view('all/list');
is_deeply( \@v => [
    { 'value' => '1', 'id' => '/default', 'key' => '/default' },
]);


$dbh->commit unless $ENV{DSN};
$dbh->disconnect;

sub do_sql {
    my $dbh = shift;
    my $sqls =<<'_END_OF_SQL_';
CREATE TABLE page_data (id text not null primary key, value text);
INSERT INTO "page_data" VALUES('_design/all','{"views":{"list":{"map":" sub { my($o,$e)=@_; $e->( $o->{_id}, 1 ) } "}}}');
INSERT INTO "page_data" VALUES('/default','{"template":"null","content":{}}');
CREATE TABLE page_map (design_id text not null, id text not null, key text not null, value text );
INSERT INTO "page_map" VALUES('_design/all/by_template','/default','null','/default');
INSERT INTO "page_map" VALUES('_design/all/list','/default','/default','1');
CREATE INDEX page_map_idx  ON page_map (design_id, key);
_END_OF_SQL_

    for my $sql ( split /\n/, $sqls ) {
        $dbh->do($sql);
    }
};

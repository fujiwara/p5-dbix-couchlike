package DBIx::CouchLike::SQLite;

use strict;
use warnings;
use base qw/ DBIx::CouchLike /;

my @sql_format = (
    q|CREATE TABLE IF NOT EXISTS %s_data (id text not null primary key, value text)|,
    q|CREATE TABLE IF NOT EXISTS %s_map (design_id text not null, id text not null, key text not null, value text )|,
    q|CREATE INDEX %s_map_idx  ON %s_map (design_id, key)|,
);

sub create_table {
    my $class = shift;
    my $dbh   = shift;
    my $name  = shift;
    for my $f (@sql_format) {
        $dbh->do( sprintf $f, $name, $name );
    }
}

sub _offset_limit_sql {
    my ( $class, $sql, $query, $param ) = @_;

    if ( defined $query->{offset} && $query->{limit} ) {
        $sql .= q{ LIMIT ?,? };
        push @$param, $query->{offset};
        push @$param, $query->{limit};
    }
    elsif ( defined $query->{limit} ) {
        $sql .= q{ LIMIT ? };
        push @$param, $query->{limit};
    }
    return $sql;
}


1;

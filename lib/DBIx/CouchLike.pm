package DBIx::CouchLike;

use strict;
use warnings;
use Carp;
use JSON 2.0 ();
use UNIVERSAL::require;
use base qw/ Class::Accessor::Fast /;
use DBIx::CouchLike::Iterator;

our $VERSION = '0.05';
our $RD;
__PACKAGE__->mk_accessors(qw/ dbh table utf8 _json /);

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new(@_);
    $self->{utf8} = 1 unless defined $self->{utf8};

    $self->{_json} = JSON->new;
    $self->{_json}->utf8( $self->{utf8} );
    _setup_downgrade() if !$self->{utf8} && !$RD;
    $self;
}

sub _setup_downgrade {
    require Unicode::RecursiveDowngrade;
    $RD = Unicode::RecursiveDowngrade->new;
    $Unicode::RecursiveDowngrade::DowngradeFunc
        = sub { utf8::downgrade($_[0]); $_[0] };
}

sub utf8 {
    my $self = shift;
    if (@_) {
        $self->{utf8} = shift;
        $self->{_json}->utf8( $self->{utf8} );
        _setup_downgrade() if !$self->{utf8} && !$RD;
    }
    $self->{utf8};
}

sub to_json {
    my $self = shift;
    my $json = $self->{_json}->encode(shift);
    utf8::downgrade($json) unless $self->{utf8};
    $json;
}

sub from_json {
    my $self = shift;
    my $json = shift;
    $self->{utf8} ? $self->{_json}->decode($json)
                  : $RD->downgrade( $self->{_json}->decode($json) );
}

sub id_generator {
    my $self = shift;
    if (@_) {
        $self->{id_generator} = shift;
    }
    $self->{id_generator} ||= do {
        my $gen;
        eval {
            require Data::YUID::Generator;
            $gen = Data::YUID::Generator->new;
        };
        unless ($gen) {
            require DBIx::CouchLike::IdGenerator;
            $gen = DBIx::CouchLike::IdGenerator->new;
        }
        $gen;
    };
}

sub sub_class {
    my $self = shift;
    return ref($self) . "::" . $self->dbh->{Driver}->{Name};
}

sub create_table {
    my $self = shift;
    return if !$self->table or !$self->dbh;

    my $sub_class = $self->sub_class;
    eval {
        $sub_class->require;
        $sub_class->create_table( $self->dbh, $self->table );
    };
    if ($@) {
        carp( "$@ Unsupported Driver: "
            . $self->dbh->{Driver}->{Name}
            . " to create_table()"
        );
    }
    1;
}

sub prepare_sql {
    my $self = shift;
    my $sql  = shift;
    $sql =~ s{_DATA_}{ $self->table . "_data" }eg;
    $sql =~ s{_MAP_}{ $self->table . "_map" }eg;
    return $self->dbh->prepare($sql);
}

sub get {
    my $self = shift;
    my $id   = shift;

    my $sth  = $self->prepare_sql(q{SELECT value FROM _DATA_ WHERE id=?});
    $sth->execute($id);
    my $r = $sth->fetchrow_arrayref;
    $sth->finish;
    return unless $r;

    my $res = $self->from_json($r->[0]);
    $res->{_id} = $id;
    return $res;
}

sub post {
    my $self = shift;
    my ( $id, $value_ref )
        = ref $_[0] ? ( delete $_[0]->{_id} || $self->id_generator->get_id, $_[0] )
                    : ( $_[0], $_[1] );
    my $sth = $self->prepare_sql(q{INSERT INTO _DATA_ (id, value) VALUES(?, ?)});
    my $json = $self->to_json($value_ref);
    $sth->execute( $id, $json )
        && $self->update_views( $id, $value_ref );
    return $id;
}

sub put {
    my $self = shift;
    my ( $id, $value_ref )
        = ref $_[0] ? ( delete $_[0]->{_id}, $_[0] )
                    : ( $_[0], $_[1] );
    my $sth  = $self->prepare_sql(q{UPDATE _DATA_ SET value=? WHERE id=?});
    my $json = $self->to_json($value_ref);
    my $r    = $sth->execute( $json, $id );
    if ( $r == 0 ) {
        return $self->post( $id, $value_ref );
    }
    return $self->update_views( $id, $value_ref );
}

sub delete {
    my $self = shift;
    my $id   = shift;
    my $sth  = $self->prepare_sql(q{DELETE FROM _DATA_ WHERE id=?});
    my $res  = $sth->execute($id);

    if ( $id =~ qr{^_design/} ) {
        my $del_sth = $self->prepare_sql(
            q{DELETE FROM _MAP_ WHERE design_id LIKE ?}
        );
        $del_sth->execute($id. "%");
    }
    else {
        my $del_sth = $self->prepare_sql(
            q{DELETE FROM _MAP_ WHERE id=?}
        );
        $del_sth->execute($id);
    };
    return $res;
}

sub view {
    my $self   = shift;
    my $target = shift;
    my $query  = shift || {};

    $target = "_design/$target"
        unless $target =~ qr{^_design/};

    my ( undef, $design_id, $name ) = split "/", $target;
    my $design = $self->get("_design/$design_id")
        or return;

    my @param = ($target);
    my $sql = q{
        SELECT m.id, m.key, m.value _COL_
        FROM _MAP_ AS m _JOIN_
        WHERE m.design_id=? };
    if ( exists $query->{key} ) {
        if ( ref $query->{key} eq 'ARRAY' ) {
            $sql .= " AND m.key IN ("
                 . join(",", map { "?" } @{ $query->{key} })
                 . ")";
            push @param, @{ $query->{key} };
        }
        elsif ( ref $query->{key} eq 'HASH' ) {
            my %k = %{ $query->{key} };
            $sql .= sprintf " AND m.key %s ? ", (keys %k)[0];
            push @param, (values %k)[0];
        }
        elsif ( ref $query->{key} eq 'SCALAR' ) {
            $sql .= sprintf " AND m.key %s", ${ $query->{key} };
        }
        else {
            $sql .= q{ AND m.key=? };
            push @param, $query->{key};
        }
    }
    elsif ( exists $query->{key_like} ) {
        $sql .= q{ AND m.key LIKE ? };
        push @param, $query->{key_like};
    }

    if ( $query->{include_docs} ) {
        $sql =~ s{_COL_}{, d.value};
        $sql =~ s{_JOIN_}{JOIN _DATA_ AS d USING(id)};
    }
    else {
        $sql =~ s{(?:_COL_|_JOIN_)}{}g;
    }

    $sql .= sprintf(
        " ORDER BY m.key %s, m.value %s, m.id ",
        $query->{key_reverse}   ? "DESC" : "",
        $query->{value_reverse} ? "DESC" : "",
    );

    $sql = $self->_offset_limit_sql( $sql, $query, \@param );

    my $sth = $self->prepare_sql($sql);
    $sth->execute(@param);

    my $itr = DBIx::CouchLike::Iterator->new({
        sth    => $sth,
        query  => $query,
        reduce => $design->{views}->{$name}->{reduce},
        couch  => $self,
    });
    return wantarray ? $itr->all()
                     : $itr;
}

sub all_designs {
    my $self  = shift;
    my $query = shift || {};
    $query->{id_like} = "_design/%";
    $self->all($query);
}

sub all {
    my $self  = shift;
    my $query = shift || {};

    my @param;
    my $sql = q{SELECT id, NULL, value FROM _DATA_};
    if ($query->{exclude_designs}) {
        $sql .= " WHERE id NOT LIKE '_design/%'";
    }
    elsif ($query->{id_like}) {
        $sql .= " WHERE id LIKE ?";
        push @param, $query->{id_like};
    }
    $sql .= " ORDER BY id";
    $sql .= " DESC" if $query->{reverse};

    $sql = $self->_offset_limit_sql( $sql, $query, \@param );
    my $sth = $self->prepare_sql($sql);
    $sth->execute(@param);

    my $itr = DBIx::CouchLike::Iterator->new({
        sth    => $sth,
        query  => $query,
        couch  => $self,
    });
    return wantarray ? $itr->all()
                     : $itr;
}

sub _offset_limit_sql {
    my $self = shift;

    my $sub;
    if ( $sub = $self->{_offset_limit_sql_sub} ) {
        return $sub->(@_);
    }
    my $sub_class = $self->sub_class;
    eval {
        $sub_class->require;
        $sub = $self->{_offset_limit_sql_sub} = sub {
            $sub_class->_offset_limit_sql(@_);
        };
    };
    if ( $sub ) {
        return $sub->(@_);
    }

    carp( "Unsupported Driver: "
        . $self->dbh->{Driver}->{Name}
        . " for using limit/offset"
    );
}

sub _select_all {
    my $self = shift;
    my $sub  = shift;
    my $sth  = $self->prepare_sql(q{SELECT id, value FROM _DATA_});
    $sth->execute();
    while ( my $r = $sth->fetchrow_arrayref ) {
        next if $r->[0] =~ qr{^_design/};
        my $id = $r->[0];
        my $value_ref = $self->from_json($r->[1]);
        $value_ref->{_id} = $id;
        $sub->( $id, $value_ref ) if $sub;
    }
}

sub create_view {
    my $self       = shift;
    my $design_id  = shift;
    my $design_val = shift;
    my $dbh        = $self->dbh;

    my $del_sth = $self->prepare_sql(
        q{DELETE FROM _MAP_ WHERE design_id LIKE ?}
    );
    $del_sth->execute($design_id . "%");

    my $views = $design_val->{views} or return 1;
    my $index_sth = $self->prepare_sql(
        q{INSERT INTO _MAP_ (design_id, id, key, value) VALUES (?,?,?,?)}
    );

 VIEW:
    for my $name ( keys %$views ) {
        my $code = $views->{$name}->{map} or next VIEW;
        my $sub = eval $code;  ## no critic
        if ($@) {
            warn $@;
            next VIEW;
        }
        $self->_select_all(
            sub {
                $self->_data_to_map({
                    sub       => $sub,
                    id        => $_[0],
                    data_val  => $_[1],
                    sth       => $index_sth,
                    design_id => "$design_id/$name",
                });
            }
        );
    }
    return 1;
}

sub update_views {
    my $self     = shift;
    my $id       = shift;
    my $data_val = shift;
    my $dbh      = $self->dbh;

    $data_val->{_id} = $id if !defined $data_val->{_id};

    return $self->create_view( $id, $data_val )
        if $id =~ qr{^_design/};

    my $del_sth = $self->prepare_sql(
        q{DELETE FROM _MAP_ WHERE id=?}
    );
    $del_sth->execute($id);

    my $index_sth = $self->prepare_sql(
        q{INSERT INTO _MAP_ (design_id, id, key, value) VALUES (?,?,?,?)}
    );

    my $sth = $self->prepare_sql(
        q{SELECT id, value FROM _DATA_ WHERE id LIKE '_design/%' }
    );
    $sth->execute();

 DESIGN:
    while ( my $r = $sth->fetchrow_arrayref ) {
        my $design_id = $r->[0];
        my $val       = $self->from_json($r->[1]);
        my $views     = $val->{views} or next DESIGN;

    VIEW:
        for my $name ( keys %$views ) {
            my $code = $views->{$name}->{map} or next VIEW;
            my $sub = eval $code;  ## no critic
            if ($@) {
                warn $@;
                next VIEW;
            }
            $self->_data_to_map({
                sub       => $sub,
                id        => $id,
                data_val  => $data_val,
                sth       => $index_sth,
                design_id => "$design_id/$name",
            });
        }
    }
    return 1;
}

sub _data_to_map {
    my $self = shift;
    my $args = shift;

    my @pair;
    my $emit = sub {
        my ( $k, $v ) = @_;
        push @pair, [
            $k,
            ref $v ? $self->to_json($v) : $v,
        ];
    };
    eval { $args->{sub}->( $args->{data_val}, $emit ) };
    if ($@) {
        warn $@;
        return;
    }
 PAIR:
    for my $p (@pair) {
        next PAIR unless defined $p->[0];
        $args->{sth}->execute(
            $args->{design_id}, $args->{id},
            $p->[0], $p->[1],
        );
    }
}


1;
__END__

=head1 NAME

DBIx::CouchLike -

=head1 SYNOPSIS

  use DBIx::CouchLike;
  use DBI;
  $dbh   = DBI->connect($dsn);
  $couch = DBIx::CouchLike->new({ dbh => $dbh, table => 'foo' });
  
  # CREATE
  $id = $couch->post({ name => 'animal', tags => ['dog', 'cat']});
  
  # CREATE (with id)
  $couch->post( $id => { name => 'animal', tags => ['dog', 'cat']} );
  # or
  $couch->post({ _id => $id, name => 'animal', tags => ['dog', 'cat']});
  
  # RETRIEVE
  $obj = $couch->get($id);
  
  # UPDATE
  $couch->put( $id, $obj );
  # or
  $couch->put( $obj ); # must be defined $obj->{_id}
  
  # DELETE
  $couch->delte($id);

  # RETRIEVE all
  @all     = $couch->all();
  @grep    = $couch->all({ id_like => "foo%" });
  @designs = $couch->all_designs();

  # define VIEW
  $map_sub_str = <<'END_OF_CODE';
  sub {
      my ($obj, $emit) = @_;
      for my $tag ( @{ $obj->{tags} } ) {
          $emit->( $tag => $obj->{name} );
      }
  }
  END_OF_CODE
  
  $reduce_sub_str = <<'END_OF_CODE';
  sub {
      my ($keys, $values) = @_;
      return scalar @$values;
  }
  END_OF_CODE
  
  $couch->post({
      _id      => "_design/find",
      views    => {
          tags => {
              map => $map_sub_str,
          },
          tags_count => {
              map    => $map_sub_str,
              reduce => $reduce_sub_str,
          },
      },
  });
  # get VIEW
  @result = $couch->view("find/tags");
  # is_deeply \@result => [ { key => "dog", value => "animal" },
  #                         { key => "cat", value => "animal" },
  #                       ]
  
  @result = $couch->view("find/tags", { key => "cat" });
  @result = $couch->view("find/tags", { include_docs => 1 });
  
  @result = $couch->view("find/tags_count");
  # is_deeply \@result => [ { key => "dog", value => 1, id => $id },
  #                         { key => "cat", value => 1, id => $id },
  #                       ]

  # get VIEW using iterator
  $itr = $couch->view("find/tags");
  $result_1 = $itr->next;
  $result_2 = $itr->next;

=head1 DESCRIPTION

DBIx::CouchLike is DBI based CouchDB like document database library.

=head1 AUTHOR

FUJIWARA E<lt>fujiwara.shunichiro gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

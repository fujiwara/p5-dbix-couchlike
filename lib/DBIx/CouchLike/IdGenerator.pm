package DBIx::CouchLike::IdGenerator;
use strict;
use warnings;
use Digest::SHA qw/ sha1_hex /;
use base qw/ Class::Accessor::Fast /;

sub get_id {
    my $self = shift;
    return sha1_hex( time() . rand() );
}

1;

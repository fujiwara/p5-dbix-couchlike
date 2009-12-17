package DBIx::CouchLike::Sth;

use strict;
use warnings;
use base qw/ Class::Accessor::Fast /;
__PACKAGE__->mk_accessors(qw/ sth sql trace /);

sub execute {
    my $self = shift;

    if ( my $h = $self->{trace} ) {
        print $h "---------------------------\n";
        print $h "$self->{sql}\n->execute(@_)\n";
        print $h "---------------------------\n";
    }
    $self->{sth}->execute(@_);
}

sub fetchrow_arrayref {
    shift->{sth}->fetchrow_arrayref(@_);
}

sub finish {
    shift->{sth}->finish(@_);
}

1;

use inc::Module::Install;
name 'DBIx-CouchLike';
all_from 'lib/DBIx/CouchLike.pm';

requires
#   'Data::YUID::Generator' => 0,
    'JSON'                  => 2,
    'UNIVERSAL::require'    => 0,
    'DBI'                   => 1,
    'Class::Accessor::Fast' => 0,
    'Unicode::RecursiveDowngrade' => 0,
;

tests 't/*.t';
author_tests('xt') if eval { require Module::Install::AuthorTests };

build_requires 'Test::More';
use_test_base;
auto_include;
WriteAll;

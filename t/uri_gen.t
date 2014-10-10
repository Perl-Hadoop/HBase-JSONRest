#!perl
use strict;
use warnings;

use Test::More tests => 1;

use HBase::JSONRest;

# testing integers
is(
    HBase::JSONRest::_build_get_uri({
      'where' => {
                   'key_equals' => 1234567890
                 },
      'versions' => 100,
      'table' => 'my_table',
      'columns' => [
                     'd:some_column_name',
                     'd:some_other_column_name'
                   ]
    })
    ,
    '/my_table/1234567890/d%3Asome_column_name,d%3Asome_other_column_name?v=100'
    ,
    q|
        HBase::JSONRest::_build_get_uri({
          'where' => {
                       'key_equals' => 1234567890
                     },
          'versions' => 100,
          'table' => 'my_table',
          'columns' => [
                         'd:some_column_name',
                         'd:some_other_column_name'
                       ]
        }) eq '/my_table/1234567890/d%3Asome_column_name,d%3Asome_other_column_name?v=100'
    |
);


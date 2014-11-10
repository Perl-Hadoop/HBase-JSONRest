#!perl
use strict;
use warnings;

use Test::More tests => 8;

use HBase::JSONRest;

# 1. simple get (no column spec, no version spec)
ok(
    HBase::JSONRest::_build_get_uri({
        'table' => 'my_table',
        'where' => {
                   'key_equals' => 1234567890
        },
    }) eq q|/my_table/1234567890|
    ,
    q|Test simple get|
);

# 2. get with versions
ok(
    HBase::JSONRest::_build_get_uri({
        'table' => 'my_table',
        'where' => {
            'key_equals' => 1234567890
        },
        'versions' => 100,
    }) eq q|/my_table/1234567890?v=100|
    ,
    q|Test versions|
);

# 3. get, specify columns, no versions (defaults to last version)
ok(
    HBase::JSONRest::_build_get_uri({
        'table' => 'my_table',
        'where' => {
            'key_equals' => 1234567890
        },
        'columns' => [
            'd:some_column_name',
            'd:some_other_column_name'
        ]
    }) eq q|/my_table/1234567890/d%3Asome_column_name,d%3Asome_other_column_name|
    ,
    q|Test columns|
);

# 4. get with versions and columns
ok(
    HBase::JSONRest::_build_get_uri({
        'table' => 'my_table',
        'where' => {
            'key_equals' => 1234567890
        },
        'versions' => 100,
        'columns' => [
            'd:some_column_name',
            'd:some_other_column_name'
        ]
    }) eq q|/my_table/1234567890/d%3Asome_column_name,d%3Asome_other_column_name?v=100|
    ,
    q|Test versions and columns|
);

# 5. get: columns, versions, timestamp range
ok(
    HBase::JSONRest::_build_get_uri({
        'table' => 'my_table',
        'where' => {
            'key_equals' => 1234567890
        },
        'versions' => 100,
        'columns' => [
            'd:some_column_name',
            'd:some_other_column_name'
        ],
        timestamp_range => {
            from  => 1415000000000,
            until => 1415300000000,
        }
    }) eq q|/my_table/1234567890/d%3Asome_column_name,d%3Asome_other_column_name/1415000000000,1415300000000?v=100|
    ,
    q|Test versions, columns and timestamp range|
); 

# 6. get: timestamp range specified without columns
ok(
    HBase::JSONRest::_build_get_uri({
        'table' => 'my_table',
        'where' => {
            'key_equals' => 1234567890
        },
        'versions' => 100,
        timestamp_range => {
            from  => 1415000000000,
            until => 1415300000000,
        }
    }) eq q|/my_table/1234567890?v=100|
    ,
    q|Test timestamp range without columns specified|
); 
# 7. simple multiget (no version spec)
ok(
    HBase::JSONRest::_build_multiget_uri({
        'table' => 'my_table',
        'where' => {
            'key_in' => [
                1234567890,
                1234567891,
                1234567892,
                1234567893,
                1234567894,
            ]
        },
    })->[0]->{url} eq q|/my_table/multiget?row=1234567890&row=1234567891&row=1234567892&row=1234567893&row=1234567894|
    ,
    q|Test simple multiget|
);

# 8. multiget: version spec
ok(
    HBase::JSONRest::_build_multiget_uri({
        'table' => 'my_table',
        'where' => {
            'key_in' => [
                1234567890,
                1234567891,
                1234567892,
                1234567893,
                1234567894,
            ]
        },
        'versions' => 3,
    })->[0]->{url} eq
    q|/my_table/multiget?row=1234567890&row=1234567891&row=1234567892&row=1234567893&row=1234567894&v=3|
    ,
    q|Test multiget with versions|
);


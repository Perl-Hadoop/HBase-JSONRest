#!perl

use strict;
use warnings;

use Test::Fatal;
use HBase::JSONRest;
use Test::More tests => 7;

like(
    exception { HBase::JSONRest->new },
    qr/^Must provide a service, or a host and port/,
    'Fails without a service or data to build a service',
);

{
    my $hb = HBase::JSONRest->new( service => 'MyService' );
    isa_ok( $hb, 'HBase::JSONRest' );
    is( $hb->{'service'}, 'MyService', 'Stored the service' );
}

{
    my $hb = HBase::JSONRest->new( host => 'MyHost' );
    isa_ok( $hb, 'HBase::JSONRest' );
    is( $hb->{'service'}, 'http://MyHost:8080', 'Created correct service' );
}

{
    my $hb = HBase::JSONRest->new( host => 'MyHost', port => 2020 );
    isa_ok( $hb, 'HBase::JSONRest' );
    is( $hb->{'service'}, 'http://MyHost:2020', 'Created correct service' );
}


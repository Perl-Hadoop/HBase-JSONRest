package HBase::JSONRest;

use strict;
use warnings;

use 5.010;

use LWP::UserAgent;
use HTTP::Request;
use CGI qw(escape);
use JSON;
use Time::HiRes qw(gettimeofday);
use MIME::Base64;
use Data::Dumper;

my %INFO_ROUTES = (
    version => '/version',
    list    => '/',
);

################
# Class Methods
#

# -------------------------------------------------------------------------
#
# new:
#
# IN: HASH => {
#   service_host => $server
# }
#
sub new {

    my $class = shift;
    $class = ref $class if ref $class;

    my $params = (ref $_[0] eq 'HASH') ? shift : {@_};

    my $service_host = delete $params->{service_host}
        || die "Need a service_host";

    my $port = delete $params->{port} || 8080;
    
    my $self->{service} = "http://$service_host:$port";

    return bless ($self, $class);

}

###################
# Instance Methods
#

# list of tables
sub list {
    my $self = shift;

    my $uri = $self->{service} . $INFO_ROUTES{list};

    my $req = HTTP::Request->new( 'GET' => $uri );

    $req->header( 'Accept' => 'application/json' );

    my $lwp = LWP::UserAgent->new;

    my $res = $lwp->request( $req );

    return( wantarray
            ? (undef, _extract_error( $res ))
            : undef
        ) unless $res->is_success;

    my $response = decode_json($res->decoded_content);

    my @tables = ();
    foreach my $table (@{$response->{table}}) {
        my $table_name = $table->{name}; # no base64
        push @tables, {name => $table_name};
    }

    return \@tables;
}

# get hbase rest version
sub version {
    my $self = shift;

    my $uri = $self->{service} . $INFO_ROUTES{version};

    my $req = HTTP::Request->new( 'GET' => $uri );

    $req->header( 'Accept' => 'application/json' );

    my $lwp = LWP::UserAgent->new;

    my $res = $lwp->request( $req );

    return( wantarray
            ? (undef, _extract_error( $res ))
            : undef
        ) unless $res->is_success;

    my $response = decode_json($res->decoded_content);

    my $version = $response->{REST} ? $response->{REST} : undef;

    return { hbase_rest_version => $version };
}

# -------------------------------------------------------------------------
#
# get
#
# usage:
#   my ($records, $err) = $hbase->get(
#        table   => 'table_name',
#        where   => {
#            key_begins_with => "key_prefix"
#        },
#    );
sub get {

    my $self = shift;
    my $query = (ref $_[0] eq 'HASH') ? shift : {@_};

    my $table = $query->{table};

    my $route;
    if ($query->{where}->{key_equals}) {
        my $key = $query->{where}->{key_equals};
        $route = '/' . $table . '/' . escape($key);
    }
    else {
        my $part_of_key = $query->{where}->{key_begins_with};
        $route = '/' . $table . '/' . escape($part_of_key . '*');
    }

    my $uri = $self->{service} . $route;

    my $req = HTTP::Request->new( 'GET' => $uri );

    $req->header( 'Accept' => 'application/json' );

    my $lwp = LWP::UserAgent->new;

    my $res = $lwp->request( $req );

    return( wantarray
            ? (undef, _extract_error( $res ))
            : undef
        ) unless $res->is_success;

    my $response = decode_json($res->decoded_content);

    my @rows = ();
    foreach my $row (@{$response->{Row}}) {

        my $key = decode_base64($row->{key});
        my @cols = ();

        foreach my $c (@{$row->{Cell}}) {
            my $name = decode_base64($c->{column});
            my $value = decode_base64($c->{'$'});
            my $ts = $c->{timestamp};
            push @cols, {name => $name, value => $value, timestamp => $ts};
        }
        push @rows, {row => $key, columns => \@cols};
    }

    return \@rows;

}

# -------------------------------------------------------------------------
#
# put_rows:
#
# IN: HASH => {
#   table   => $table,
#   changes => [ # array of hashes, where each hash is one row
#       ...,
#       {
#          row_key   => "$row_key",
#          row_cell => [
#              { column => 'family:name', value => 'value' },
#              ...,
#              { column => 'family:name', value => 'value' },
#         ],
#      },
#      ...
#   ]
# }
#
# OUT: result flag
sub put_rows {
    my $self    = shift;
    my $command = (ref $_[0] eq 'HASH') ? shift : {@_};

    # at least one valid record
    unless ($command->{table} && $command->{changes}->[0]->{row_key} && $command->{changes}->[0]->{row_cells}) {
        die q/Must provide required parameters:
            IN: HASH => {
               table   => $table,
               changes => [
                   ...,
                   {
                      row_key   => "$row_key",
                      row_cells => [
                          { column => 'family:name', value => 'value' },
                          ...
                          { column => 'family:name', value => 'value' },
                     ],
                  },
                  ...
               ]
             };
        /;
    }

    my $table   = $command->{table};

    # build JSON:
    my $JSON_Command .= '{"Row":[';
    my @sorted_json_row_changes = ();
    foreach my $row_change (@{$command->{changes}}) {

        my $row_cell_changes   = $row_change->{row_cells};

        my $rows = [];
        my $row_change_formated = { Row => $rows };
        my $row_cell_changes_formated = {};

        my $ts = int(gettimeofday * 1000);

        # hbase wants keys in sorted order; it wont work otherwise;
        # more specificaly, the special key '$' has to be at the end;
        my $sorted_json_row_change =
            q|{"key":"|
            . encode_base64($row_change->{row_key}, '')
            . q|","Cell":[|
        ;

        my @sorted_json_cell_changes = ();
        foreach my $cell_change (@$row_cell_changes) {

            my  $sorted_json_cell_change =
                    '{'
                        . '"timestamp":"'
                        . $ts
                        . '",'
                        . '"column":"'
                        . encode_base64($cell_change->{column}, '')
                        . '",'
                        . '"$":"'
                        . encode_base64($cell_change->{value}, '')
                    . '"}'
            ;

            push @sorted_json_cell_changes, $sorted_json_cell_change;

        } # next Cell

        $sorted_json_row_change .= join(",", @sorted_json_cell_changes);
        $sorted_json_row_change .= ']}';

        push @sorted_json_row_changes, $sorted_json_row_change;

    } # next Row

    $JSON_Command .= join(",", @sorted_json_row_changes);
    $JSON_Command .= ']}';

    my $route = '/' . escape($table) . '/false-row-key';
    my $uri = $self->{service} . $route;

    my $req = HTTP::Request->new( 'PUT' => $uri );

    $req->header(  'Accept' => 'application/json');

    $req->content_type('application/json');
    $req->content($JSON_Command);

    my $lwp = LWP::UserAgent->new;
    my $res = $lwp->request($req);

    return wantarray
        ? ($res->is_success, _extract_error($res))
        : $res->is_success;
}

# _extract_error
sub _extract_error {
    my $res = shift;

    return if $res->is_success;
    return if $res->code == 404;

    my $msg = $res->message;

    my ($exception, $info) = $msg =~ m{\.([^\.]+):(.*)$};
    if ($exception) {
        $exception =~ s{Exception$}{};    
    } else {
        $exception = 'SomeOther - not set?';
        $info = $msg || $res->code || $res->as_string;
    }

    return { type => $exception, info => $info };
}

1;

package HBase::JSONRest;

use strict;
use warnings;

use Carp;
use HTTP::Tiny;
use URI::Escape;
use MIME::Base64;
use JSON::XS qw(decode_json encode_json);
use Time::HiRes qw(gettimeofday time);
use Data::Dumper;

our $VERSION = "0.011";

my %INFO_ROUTES = (
    version => '/version',
    list    => '/',
);

################
# Class Methods
#
sub new {
    my $class  = shift;
    my %params = @_;

    # default port
    $params{'port'} ||= 8080;

    # missing service, we'll create it ourselves
    if ( ! defined $params{'service'} ) {
        # but we need a host for that
        defined $params{'host'}
            or croak 'Must provide a service, or a host and port';

        # set it up
        $params{'service'} =
            sprintf 'http://%s:%d', @params{qw<host port>};
    }

    # we only care about the service, and we assured it exists
    return bless { service => $params{'service'} }, $class;
}


###################
# Instance Methods
#

# -------------------------------------------------------------------------
#
# list of tables
#
sub list {
    my $self = shift;

    $self->{last_error} = undef;

    my $uri = $self->{service} . $INFO_ROUTES{list};

    my $http = HTTP::Tiny->new();

    my $rs = $http->get($uri, {
         headers => {
             'Accept' => 'application/json',
         }
    });

    if ( ! $rs->{success} ) {
       $self->{last_error} = _extract_error_tiny($uri, $rs);
       return undef;
    }

    my $response = decode_json($rs->{content});

    my @tables = ();
    foreach my $table (@{$response->{table}}) {
        my $table_name = $table->{name};
        push @tables, {name => $table_name};
    }

    return \@tables;
}

# -------------------------------------------------------------------------
#
# get hbase rest version info
#
sub version {
    my $self = shift;

    $self->{last_error} = undef;

    my $uri = $self->{service} . $INFO_ROUTES{version};

    my $http = HTTP::Tiny->new();

    my $rs = $http->get($uri, {
         headers => {
             'Accept' => 'application/json',
         }
    });

    if ( ! $rs->{success} ) {
       $self->{last_error} = _extract_error_tiny($uri, $rs);
       return undef;
    }

    my $response = decode_json($rs->{content});

    return $response;
}

# -------------------------------------------------------------------------
#
# get
#
# usage:
#
# my $records = $hbase->get({
#   table => $table_name,
#   where => {
#        key_equals => $key
#   },
#   columns => [
#       'd:some_column_name',
#       'd:some_other_column_name'
#   ],
#   versions => 100,
#   timestamp_range => {
#       from  => $timestamp_from,
#       until => $timestamp_until,
#   }
# });
#
sub get {
    my $self   = shift;
    my $params = shift;

    $self->{last_error} = undef;

    my $uri = _build_get_uri($params);

    my $rows = $self->_get_tiny($uri);

    return $rows;
}

# _get_tiny
sub _get_tiny {

    my $self = shift;
    my $uri  = shift;

    my $url = $self->{service} . $uri;

    my $http = HTTP::Tiny->new();

    my $rs = $http->get($url, {
        headers => {
            'Accept' => 'application/json',
        }
    });

    if ( ! $rs->{success} ) {
       $self->{last_error} = _extract_error_tiny($url, $rs);
       return undef;
    }

    my $response = decode_json($rs->{content});

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
# multiget
#
# usage:
#
# my $records = $hbase->multiget({
#    table   => $table_name,
#    where   => {
#        key_in => \@keys
#    },
#    versions => $number_of_versions,
# });
#
sub multiget {
    my $self  = shift;
    my $query = shift;

    $self->{last_error} = undef;

    my $where = $query->{where};
    unless ($where->{key_in} && @{$where->{key_in}}) {
        $self->{last_error} = {
            type => "Bad request",
            info => "No keys specified for multiget.",
            uri  => "Could not counstruct uri - no keys provided.",
        };
        return;
    }

    my $multiget_urls = _build_multiget_uri($query);

    my @result = ();
    foreach my $url (@$multiget_urls) {
        my $rows = $self->_multiget_tiny($url->{url});
        push @result, @$rows
            if ($rows and @$rows);
    }

    return \@result;
}

# -------------------------------------------------------------------------
#
# _multiget_tiny
#
sub _multiget_tiny {

    my $self = shift; # hbh
    my $uri  = shift;

    my $url = $self->{service} . $uri;

    my $http = HTTP::Tiny->new();

    my $data_format = 'application/json';

    my $rs = $http->get($url, {
        headers => {
            'Accept' => $data_format,
        }
    });

    if ( ! $rs->{success} ) {
       $self->{last_error} = _extract_error_tiny($url, $rs);
       return undef;
    }

    my $response = decode_json($rs->{content});

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
# put:
#
# IN: HASH => {
#   table   => $table,
#   changes => [ # array of hashes, where each hash is one row
#       ...,
#       {
#          row_key   => "$row_key",
#          row_cell => [
#              {
#                   column    => "$family:$name",
#                   value     => "$value",
#                   timestamp => $timestamp # <- optional (override HBase timestamp) 
#               },
#              ...,
#              { column => "$family:$name", value => "$value" },
#         ],
#      },
#      ...
#   ]
# }
#
# OUT: result flag
sub put {
    my $self    = shift;
    my $command = shift;

    $self->{last_error} = undef;

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

        # hbase wants keys in sorted order; it wont work otherwise;
        # more specificaly, the special key '$' has to be at the end;
        my $sorted_json_row_change =
            q|{"key":"|
            . encode_base64($row_change->{row_key}, '')
            . q|","Cell":[|
        ;

        my @sorted_json_cell_changes = ();
        foreach my $cell_change (@$row_cell_changes) {

            # timestamp override
            my $ts;
            my $overide_timestamp = undef;
            if ($cell_change->{timestamp}) {
                $overide_timestamp = 1;
                $ts = $cell_change->{timestamp};
            }

            my $timestamp_override_string = $overide_timestamp
                ? '"timestamp":"' . $ts . '",'
                : ''
            ;

            my  $sorted_json_cell_change =
                    '{'
                        . $timestamp_override_string
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

    my $route = '/' . uri_escape($table) . '/false-row-key';
    my $uri = $self->{service} . $route;

    my $http = HTTP::Tiny->new();

    my $rs = $http->request('PUT', $uri, {
        content => $JSON_Command,
        headers => {
            'Accept'       => 'application/json',
            'content-type' => 'application/json'
        },
    });

    if ( ! $rs->{success} ) {
       $self->{last_error} = _extract_error_tiny($uri, $rs);
       return undef;
    }

}

# =========================================================================
# delete: delete an entire record or selected columns of it
#
# Usage:
# my $success = $hbh->delete({
#   table    => 'table',
#   key      => 'key',
#   family   => 'family', # optional, unless column is given
#   column   => 'column', # optional
# })
sub delete {
    my ($self, $attr) = @_;
    my $table  = delete $attr->{table};
    my $key    = delete $attr->{key};
    my $family = delete $attr->{family};
    my $column = delete $attr->{column};
    my ($route, $http, $rs, $url);

    die "Table name required" if(!$table);
    die "Row key required" if(!$key);
    die "Family is required if column is given" if($column && !$family);

    $key = join(';', @$key) if(ref($key) eq 'ARRAY');
    $route = sprintf("/%s/%s", $table, uri_escape($key));

    if($family) {
        $route .= sprintf("/%s", $family);
        $route .= sprintf(":%s", $column) if($column);
    }

    $http = HTTP::Tiny->new();
    $url = sprintf("%s%s", $self->{service}, $route);
    $rs = $http->delete($url, {
        headers => {
            'Accept' => 'application/json',
        }
    });

    if($rs->{success}) {
       $self->{last_error} = undef;
       return 1;
    }
    else {
        $self->{last_error} = _extract_error_tiny($url, $rs);
        return 0;
    }
}

# -------------------------------------------------------------------------
# build get uri
#
sub _build_get_uri {
    my $query = shift;

    my $table = $query->{table};

    my $columns_url_part = "";
    if ( $query->{columns} and @{$query->{columns}} ) {
        $columns_url_part = "/" . join(",", map { uri_escape($_) } @{ $query->{columns} });
    }

    my $timestamp_url_part  = undef;
    # timestamp range query is supported only if columns are specifed
    if ($query->{columns} and @{$query->{columns}}) {
        if ( $query->{timestamp_range} and %{ $query->{timestamp_range} } ) {
            my $timestamp_from  = $query->{timestamp_range}->{from};
            my $timestamp_until = $query->{timestamp_range}->{until};
            $timestamp_url_part = "/" . $timestamp_from . "," . $timestamp_until;
        }
    }

    my $versions_url_part = undef;
    if ( $query->{versions} ) {
        my $versions = $query->{versions};
        $versions_url_part = "?v=$versions";
    }

    my $uri;
    if ($query->{where}->{key_equals}) {
        my $key = $query->{where}->{key_equals};

        $uri = '/' . $table . '/' . uri_escape($key) . $columns_url_part;

        $uri .= $timestamp_url_part if $timestamp_url_part;
        $uri .= $versions_url_part if $versions_url_part;
    }
    else {
        my $part_of_key = $query->{where}->{key_begins_with};
        $uri = '/' . $table . '/' . uri_escape($part_of_key . '*') . $columns_url_part;

        $uri .= $timestamp_url_part if $timestamp_url_part;
        $uri .= $versions_url_part if $versions_url_part;
    }

    return $uri;
}

# -------------------------------------------------------------------------
# build multiget url
#
sub _build_multiget_uri {
    my $query = shift;

    my $keys  = $query->{where}->{key_in};
    my $table = $query->{table};

    my $route_base = '/' . $table . '/multiget?';
    my $uri_base = $route_base;

    my @multiget_urls = ();
    my $current_url = undef;
    foreach my $key (@$keys) {
        if (! defined $current_url) {
            $current_url ||= $uri_base . "row=" . $key;
        }
        else{
            my $next_url = $current_url . '&row=' . $key;
            if (length($next_url) < 2000) {
                $current_url = $next_url;
            }
            else {
                push @multiget_urls, { url => $current_url, len => length($current_url) };
                $current_url = undef;
            }
        }
    }
    # last batch
    push @multiget_urls, { url => $current_url, len => length($current_url) };

    if ($query->{versions}) {
        foreach my $mget_url (@multiget_urls) {
            my $versions = $query->{versions};
            my $versions_url_part = "v=$versions";
            
            $mget_url->{url} .= '&' . $versions_url_part;
        }
    }

    return \@multiget_urls;
}

# -------------------------------------------------------------------------
# parse error
#
sub _extract_error_tiny {

    my $uri = shift;
    my $res = shift;

    return if $res->{success};

    if (!$res->{status}) {
         return {
            type => 'Unkown',
            info => 'No status in response',
            uri => $uri
        };
    }

    if ($res->{status} and $res->{status} == 404) {
        return {
            type => '404',
            info => 'No rows found',
            uri => $uri
        };
    }

    my $msg;
    if ($res->{reason}) {
        $msg = $res->{reason};
    }
    else {
        return {
            type => "Bad response",
            info => "No reason in the response",
            uri  => $uri
        };
    }

    my ($exception, $info) = $msg =~ m{\.([^\.]+):(.*)$};
    if ($exception) {
        $exception =~ s{Exception$}{};
    } else {
        $exception = 'SomeOther - not set?';
        $info = $msg || $res->{status} || Dumper($res);
    }

    my $error_tiny = { type => $exception, info => $info, uri => $uri };

    if ($info =~ m/Service Unavailable/) {
        $error_tiny->{guess} = 'Undefined table?';
    }

    return $error_tiny;
}

1;

__END__

=encoding utf8

=head1 NAME

HBase::JSONRest - Simple REST client for HBase

=head1 SYNOPSIS

A simple get request:

    my $hbase = HBase::JSONRest->new(host => $hostname);

    my $records = $hbase->get({
        table   => 'table_name',
        where   => {
            key_begins_with => "key_prefix"
        },
    });

A simple put request:

    # array of hashes, where each hash is one row
    my $rows = [
        ...
        {
            row_key => "$row_key",

            # cells: array of hashes where eash hash is one cell
            row_cells => [
              { column => "$family_name:$colum_name", value => "$value" },
            ],
       },
       ...
    ];

    my $res = $hbase->put({
        table   => $table_name,
        changes => $rows
    });

=head1 DESCRIPTION

A simple rest client for HBase.

=head1 METHODS

=head2 new

Constructor. Cretes an hbase client object that is used to talk to HBase.

    my $hostname = "hbase-restserver.mydomain.com";
    
    my $hbase = HBase::JSONRest->new(host => $hostname);

=head2 get

Scans a table by key prefix or exact key match depending on options passed:

    # scan by key prefix:
    my $records = $hbase->get({
        table       => $table_name,
        where       => {
            key_begins_with => "$key_prefix"
        },
    });

    # exact key match: get the whole row
    my $record = $hbase->get({
        table       => $table_name,
        where       => {
            key_equals => "$key"
        },
    });

    # exact key match: get only specific columns
    my $record = $hbase->get({
      table => $table_name,
      where => {
           key_equals => $key
      },
      columns => [
          'd:some_column_name',
          'd:some_other_column_name'
      ],
    });

    # exact key match: get last $N cell versions
    my $records = $hbase->get({
      table => $table_name,
      where => {
           key_equals => $key
      },
      columns => [
          'd:some_column_name',
          'd:some_other_column_name'
      ],
      versions => $N,
    });

    # exact key match: get cell versions created within a timestamp range
    my $records = $hbase->get({
      table => $table_name,
      where => {
           key_equals => $key
      },
      columns => [
          'd:some_column_name',
          'd:some_other_column_name'
      ],
      timestamp_range => {
          from  => $timestamp_from,
          until => $timestamp_until,
      }
    });

=head2 multiget

Does a multiget request to HBase, so that multiple keys can be matched
in one http request. It also makes sure that the request url is not longer
than 2000 chars, so if the number of keys passed is large enough and would
result in url longer than 2000 chars, the request is split into multiple
smaller request so each is shorter than 2000 chars.

    # multiget: get only last cell version from matched rows
    my @keys = ($key1,...,$keyN);
    my $records = $hbase->multiget({
        table   => $table_name,
        where   => {
            key_in => \@keys
        },
    });

    # multiget: get last $N cell versions from matched rows
    my @keys = ($key1,...,$keyN);
    my $records = $hbase->multiget({
        table   => $table_name,
        where   => {
            key_in => \@keys
        },
        versions => $N,
    });

=head2 put

Inserts one or multiple rows. If a key allready exists then depending
on if HBase versioning is ON for that specific table, the record will
be updated (versioning is off) or new version will be inserted (versioning
is on)

    # multiple rows
    my $rows = [
        ...
        {
            row_key => "$row_key",

            # cells: array of hashes where eash hash is one cell
            row_cells => [
                {
                    column => "$family_name1:$colum_name1",
                    value  => "$value1",
                    timestamp => "$timestamp1", # <- optional (override HBase timestamp)
                },
                ...,
                {
                    column => "$family_nameN:$colum_nameN",
                    value  => "$valueN",
                    timestamp => "$timestampN", # <- optional (override HBase timestamp)
                },
            ],
       },
       ...
    ];

    my $res = $hbase->put({
        table   => $table_name,
        changes => $rows
    });

    # single row - basically the same as multiple rows, but
    # the rows array has just one elements
    my $rows = [
        {
            row_key => "$row_key",

            # cells: array of hashes where eash hash is one cell
            row_cells => [
              { column => "$family_name:$colum_name", value => "$value" },
            ],
       },
    ];

    my $res = $hbase->put({
        table   => $table_name,
        changes => $rows
    });

=head2 delete

Deletes an entire record or selected columns of it

    my $success = $hbase->delete({
        table    => 'table',
        key      => 'key',
        family   => 'family', # optional, unless column is given
        column   => 'column', # optional
    });

=head2 version

Returns a hashref with server version info

    my $version_info = $hbase->version;
    print Dumper($version_info);

    output example:
    ---------------
    version => $VAR1 = {
          'Server' => 'jetty/6.1.26.cloudera.2',
          'Jersey' => '1.8',
          'REST' => '0.0.2',
          'OS' => 'Linux 2.6.32-358.23.2.el6.x86_64 amd64',
          'JVM' => 'Oracle Corporation 1.7.0_51-24.51-b03'
        };

=head2 list

Returns a list of tables available in HBase

    print "tables => " . Dumper($hbase->list);

    tables => $VAR1 = [
              {
                'name' => 'very_big_table'
              },
              ..., 
              {
                'name' => 'super_big_table'
              }
            ];

=head1 ERROR HANDLING

Information on error is stored in hbase object under key last error:

    my $records = $hbase->get({
        table       => $table_name,
        where       => {
            key_begins_with => "$key_prefix"
        },
    });
    if ($hbase->{last_error}) {
        # handle error    
    }
    else {
        # process records    
    }

=head1 VERSION

Current version: 0.011

=head1 AUTHOR

bdevetak - Bosko Devetak (cpan:BDEVETAK) <bosko.devetak@gmail.com>

=head1 CONTRIBUTORS

theMage, C<<  <cpan:NEVES> >>, <mailto:themage@magick-source.net>

Sawyer X, C<< <xsawyerx at cpan.org> >>

Eric Herman, C<< <eherman at cpan.org> >>

=head1 COPYRIGHT

Copyright (c) 2014 the HBase::JSONRest L</AUTHOR> and L</CONTRIBUTORS>
as listed above.

=head1 LICENSE

This library is free software and may be distributed under the same terms
as perl itself. See L<http://dev.perl.org/licenses/>.

=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

=cut


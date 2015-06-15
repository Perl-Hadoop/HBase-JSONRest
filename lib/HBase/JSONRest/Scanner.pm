package HBase::JSONRest::Scanner;

use strict;
use warnings;

use URI::Escape;
use Time::HiRes qw(time);

# new
sub new {
    my $class  = shift;
    my $params = shift;

    die "HBase handle required!"
        unless ($params->{hbase} and (ref $params->{hbase}));

    my $hbase = $params->{hbase};

    my $first_key = $params->{start_key};

    my $limit     = $params->{atatime} || 50;
    my $batchsize = $params->{_batchsize} || 50;

    my $self = {
        hbase        => $hbase,

        table      => $params->{table},
        prefix     => $params->{prefix},

        first_key  => $params->{first_key},
        last_key_from_previous_batch => undef,

        limit     => $limit,
        batchsize => $batchsize,
    };

    return bless $self, $class;
}

# get_next_batch
sub get_next_batch {

    my $self = shift;

    $self->{_last_batch_time_start} = time;

    my $table  = $self->{table};
    my $prefix = $self->{prefix};
    my $limit  = $self->{limit};
    my $hbase  = $self->{hbase};

    my $last_key_from_previous_batch;

    if (!$self->{first_key}) {

        my $first_row = $self->_get_first_row_of_prefix();

        return undef if (!$first_row && !$first_row->{row}); # no rows for that prefix

        $self->{first_key} = $first_row->{row};

        my $rows = $self->_scan_raw({
            table      => $self->{table},
            startrow   => $self->{first_key}, # <- inclusive
            limit      => $limit,
            batchsize  => $self->{batchsize},
        });

        if (!$hbase->{last_error}) {
            $self->{last_key_from_previous_batch} = $rows->[-1]->{row};
            $self->{last_batch_time} = time - $self->{_last_batch_time_start};
            return $rows;
        }
        else {
            die "Error while trying to get the first key of a prefix!" . Dumper($hbase->{last_error});
        }
    }
    else {

        return undef if !$self->{last_key_from_previous_batch}; # no more records

        $last_key_from_previous_batch = $self->{last_key_from_previous_batch};
        $self->{last_key_from_previous_batch} = undef;

        my $next_batch = $self->_scan_raw({
            table     => $table,

            # inclusive scan for startrow, so we add x to skip the key that
            # was allready returned in previous batch
            startrow  => $last_key_from_previous_batch . "x",

            limit     => $limit,
            batchsize => $self->{batchsize},
        });

        if (!$hbase->{last_error}) {
            $self->{last_key_from_previous_batch} = $next_batch->[-1]->{row};
            $self->{last_batch_time} = time - $self->{_last_batch_time_start};
            return $next_batch;
        }
        else {
            die "Scanner error while trying to get next batch!"
                . Dumper($hbase->{last_error});
        }
    }
}

# _get_first_row_of_prefix
sub _get_first_row_of_prefix {
    my $self = shift;

    my $prefix = $self->{prefix};
    my $hbase  = $self->{hbase};
    my $table  = $self->{table};

    my $rows = $self->_scan_raw({
        table     => $table,
        rowprefix => $prefix,
        limit     => 1,
    });

    die "Should be only one first row!"
        if ( scalar @$rows > 1);

    return undef unless $rows->[0];

    my $first_row = $rows->[0];

    return $first_row;
}

# _scan_raw (uses passed paremeters instead of instance parameters)
sub _scan_raw {
    my $self   = shift;
    my $params = shift;

    my $hbase = $self->{hbase};
    $hbase->{last_error} = undef;

    my $scan_uri = $self->_build_scan_uri($params);

    my $rows = $hbase->_get_tiny($scan_uri);

    return $rows;
}

sub _build_scan_uri {
    my $self   = shift;
    my $params = shift;

    #
    #    request parameters:
    #
    #    1. startrow - The start row for the scan.
    #    2. endrow - The end row for the scan.
    #    3. columns - The columns to scan.
    #    4. starttime, endtime - To only retrieve columns within a specific range of version timestamps,both start and end time must be specified.
    #    5. maxversions - To limit the number of versions of each column to be returned.
    #    6. batchsize - To limit the maximum number of values returned for each call to next().
    #    7. limit - The number of rows to return in the scan operation.

    my $table       = $params->{table};
    my $batchsize   = $params->{batchsize}   || 10;
    my $limit       = $params->{limit}       || 10;

    # optional
    my $startrow    = $params->{startrow}    || "";
    my $rowprefix   = $params->{rowprefix}   || "";
    my $endrow      = $params->{endrow}      || "";
    my $columns     = $params->{columns}     || "";

    my $starttime   = $params->{starttime}   || "";
    my $endtime     = $params->{endtime}     || "";

    my $maxversions = $params->{maxversions} || "";

    # simple version: only mandatory parameters used (and rowprefix)
    my $uri;

    if ($rowprefix) {

        $uri = "/"
             . uri_escape($table)
             . "/"
             . uri_escape($rowprefix)
             . '*'
             . "?limit="     . $limit
             . "&batchsize=" . $batchsize
        ;
    }
    elsif (!$rowprefix && $startrow) {
        $uri
            = "/"
            . uri_escape($table)
            . "/"
            . '*?'
            . "startrow="   . uri_escape($startrow)
            . "&limit="     . $limit
            . "&batchsize=" . $batchsize
        ;
    }
    else {
        die "unsupported option!";
    }

    return $uri;
}

1;

__END__

=encoding utf8

=head1 NAME

HBase::JSONRest::Scanner - Simple client for HBase stateless REST scanners

=head1 SYNOPSIS

A simple scanner:

    use HBase::JSONRest;

    my $hbase = HBase::JSONRest->new(host => 'my-rest-host');

    my $table       = 'name of table to scan';
    my $prefix      = 'key prefix to scan';
    my $batch_size  = 100; # rows per one batch

    my $scanner = HBase::JSONRest::Scanner->new({
        hbase   => $hbase,
        table   => $table,
        prefix  => $prefix,
        atatime => $batch_size,
    });

    my $rows;
    while ($rows = $scanner->get_next_batch()) {
        print STDERR "got "
            . @$rows . " rows in "
            . sprintf("%.3f", $scanner->{last_batch_time}) . " seconds\n\n";
        print STDERR "first key in batch ==> " . $rows->[0]->{row} . "\n";
        print STDERR "last key in batch  ==> " . $rows->[-1]->{row} . "\n";
    }

=head1 DESCRIPTION

Simple client for HBase stateless REST scanners.

=head1 METHODS

=head2 new

Constructor. Cretes an HBase stateless REST scanner object.

    my $scanner = HBase::JSONRest::Scanner->new({
        hbase   => $hbase,
        table   => $table,
        prefix  => $prefix,
        atatime => $batch_size,
    });

=head2 get_next_batch

Gets the next batch of records

    while ($rows = $scanner->get_next_batch()) {
        ...
    }

=head1 VERSION

Current version: 0.030

=head1 AUTHOR

bdevetak - Bosko Devetak (cpan:BDEVETAK) <bosko.devetak@gmail.com>

=head1 CONTRIBUTORS

theMage, C<<  <cpan:NEVES> >>, <mailto:themage@magick-source.net>

Sawyer X, C<< <xsawyerx at cpan.org> >>

Eric Herman, C<< <eherman at cpan.org> >>

Robert Nilsson, <rn@orbstation.com>

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

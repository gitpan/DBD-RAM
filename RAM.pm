#########################################################################
#
#   DBD::RAM - a DBI driver for in-memory data structures
#
#   This module is copyright (c), 2000 by Jeff Zucker
#   All rights reserved.
#
#   You may distribute this module under the same terms as Perl
#   itself as specified in the Perl README file.
#
#   WARNING: this is alpha software, no warranty of any kind is implied.
#
#   To learn more: enter "perldoc DBD::RAM" at the command prompt,
#   or search in this file for =head1 and read the text below it
#
#########################################################################

package DBD::RAM;

use strict;
require DBD::File;
require SQL::Statement;
require SQL::Eval;
use IO::File;
use Text::ParseWords;

use vars qw($VERSION $err $errstr $sqlstate $drh $ramdata);

$VERSION = '0.05';

$err       = 0;        # holds error code   for DBI::err
$errstr    = "";       # holds error string for DBI::errstr
$sqlstate  = "";       # holds SQL state for    DBI::state
$drh       = undef;    # holds driver handle once initialized

sub driver {
    return $drh if $drh;        # already created - return same one
    my($class, $attr) = @_;
    $class .= "::dr";
    $drh = DBI::_new_drh($class, {
        'Name'    => 'RAM',
        'Version' => $VERSION,
        'Err'     => \$DBD::RAM::err,
        'Errstr'  => \$DBD::RAM::errstr,
        'State'   => \$DBD::RAM::sqlstate,
        'Attribution' => 'DBD::RAM by Jeff Zucker',
    });
    return $drh;
}

package DBD::RAM::dr; # ====== DRIVER ======

$DBD::RAM::dr::imp_data_size = 0;

@DBD::RAM::dr::ISA = qw(DBD::File::dr);

sub connect {
    my($drh, $dbname, $user, $auth, $attr)= @_;
    my $dbh = DBI::_new_dbh($drh, {
        Name         => $dbname,
        USER         => $user,
        CURRENT_USER => $user,
    });
    # UNUSED, BUT CAN ADD EXTRA STRINGS ONTO DSN HERE EVENTUALLY
    # Process attributes from the DSN; we assume ODBC syntax
    # here, that is, the DSN looks like var1=val1;...;varN=valN
    my $var;
    foreach $var (split(/;/, $dbname)) {
        if ($var =~ /(.*?)=(.*)/) {
            my $key = $1;
            my $val = $2;
            $dbh->STORE($key, $val);
        }
    }
    $dbh->STORE('f_dir','./') if !$dbh->{f_dir};
    $dbh;
}

sub data_sources {}

sub disconnect_all{ undef $DBD::RAM::ramdata;}

sub DESTROY {undef $DBD::RAM::ramdata;}


package DBD::RAM::db; # ====== DATABASE ======

$DBD::RAM::db::imp_data_size = 0;

@DBD::RAM::db::ISA = qw(DBD::File::db);

sub disconnect{ undef $DBD::RAM::ramdata;}

# DRIVER PRIVATE METHODS

sub get_catalog {
    my $self  = shift;
    my $tname = shift || '';
    my $catalog = $DBD::RAM::ramdata->{catalog}{$tname} || {};
    $catalog->{f_type}   |= '';
    $catalog->{f_name}    |= '';
    $catalog->{pattern}   |= '';
    $catalog->{col_names} |= '';
    return $catalog;
}

sub catalog {
    my $dbh = shift;
    my $table_info = shift;
    if (!$table_info) {
        my @tables = (keys %{$DBD::RAM::ramdata->{catalog}} );
        my @all_tables;
        for (@tables) {
            push @all_tables,[
                $_,
                $DBD::RAM::ramdata->{catalog}{$_}{f_type},
                $DBD::RAM::ramdata->{catalog}{$_}{f_name},
                $DBD::RAM::ramdata->{catalog}{$_}{pattern},
                $DBD::RAM::ramdata->{catalog}{$_}{col_names},
                $DBD::RAM::ramdata->{catalog}{$_}{read_sub},
                $DBD::RAM::ramdata->{catalog}{$_}{write_sub}];
        }
        return @all_tables;
    }
    for (@{$table_info}) {
        my($table_name,$f_type,$f_name,$hash) = @{$_};
        $DBD::RAM::ramdata->{catalog}{$table_name}{f_type} = uc $f_type || '';
        $DBD::RAM::ramdata->{catalog}{$table_name}{f_name} = $f_name    || '';
        if ($hash) {
	    for(keys %{$hash}) {
               $DBD::RAM::ramdata->{catalog}{$table_name}{$_}=$hash->{$_};
 	    }
	}
    }
}
sub get_table_name {
    my $dbh = shift;
    my @tables = (keys %{$DBD::RAM::ramdata} );
    if (!$tables[0]) { return 'table1';  }
    my $next=0;
    for my $table(@tables) {
        if ($table =~ /^table(\d+)/ ) {
            $next = $1 if $1 > $next;
        }
    }
    $next++;
    return("table$next");
}


sub export() {
    # NEEDS WORK (AN UNDERSTATEMENT)
    my $dbh    = shift;
    my $sql    = shift;
    my $f_name = shift;
    my $f_type = shift || 'CSV';
    $dbh->func('tmp__',$f_type,$f_name,'catalog');
    for (@{$sql}) {
        my $sth = $dbh->prepare($sql);
        $sth->execute;
        while (my @row = $sth->fetchrow_array) {
        }
    }
}

sub import() {
    my $dbh   = shift;
    my $specs = shift;
    my $data  = shift;
    if ($specs && ! $data ) {
        if (ref $specs eq 'ARRAY' ) {
            $data = $specs; $specs = {};
        }
        else {
	    $data = [];
	}
    }
    if (ref $specs ne 'HASH') {
        die 'First argument to "import" must be a hashref.';
    }
    if (ref $data ne 'ARRAY') {
        die 'Second argument to "import" must be an arrayref.';
    }
    my $data_type  = uc $specs->{data_type}  || 'CSV';
    my $table_name = $specs->{table_name} || $dbh->func('get_table_name');
    my $col_names  = $specs->{col_names}  || '';
    my $pattern    = $specs->{pattern}    || '';
    my $parser     = $specs->{parser}     || '';
    if ($data_type eq 'MP3' ) {
#      use Data::Dumper; print Dumper $specs; exit;
        $data_type = 'FIX';
        $col_names = 'file_name,song_name,artist,album,year,comment,genre',
        $pattern   = 'A255 A30 A30 A30 A4 A30 A50',
        $data      = &get_music_library( $specs->{dirs} )
    }
    my($colstr,@col_names,$num_params);
    if ( $col_names ) {
        $col_names =~ s/\s+//g;
         @col_names = split /,/,$col_names;
    }
    else {
        if ( $data_type eq 'FIX' ) {
            my @x = split /\s+/,$pattern;
            $num_params = scalar @x;
	}
        if ( $data_type eq 'CSV' ) {
            my @colAry = $dbh->func(@{$data}->[0],
                                    $table_name,'CSV','read_fields');
            $num_params = scalar @colAry;
	}
        $num_params = scalar @{ @{$data}->[0] } if !$num_params;
        for ( 1 .. $num_params ) { push(@col_names,"col$_"); }
    }
    for ( @col_names ) { $colstr .= $_ . ' TEXT,'; }
    $colstr =~ s/,$//;
    my $sql = "CREATE TABLE $table_name ($colstr)";
    $dbh->do($sql);
    my $param_str = '';
    $num_params = scalar @col_names;
    for (1..$num_params) {
        $param_str .= '?,';
    }
    $param_str =~ s/,$//;
    $sql = "INSERT INTO $table_name VALUES ($param_str)";
    my $sth = $dbh->prepare($sql);
    if ( $data_type eq 'FIX' ) {
        for ( @{$data} ) {
            my @row = unpack $pattern, $_;
            $sth->execute(@row);
        }
    }
    if ( $data_type eq 'CSV' ) {
        for ( @{$data} ) {
            my @datarow = $dbh->func($_,$table_name,'CSV','read_fields');
            $sth->execute(@datarow);
        }
    }
    if ( $data_type eq 'ARRAY' ) {
        for ( @{$data} ) {
            $sth->execute(@{$_});
        }
    }
    if ( $data_type eq 'HASH' ) {
        for ( @{$data} ) {
            my @datarow;
            my %rowhash = %{$_};
            for (@col_names) {
                push @datarow, $rowhash{$_};
  	    }
            $sth->execute(@datarow);
        }
    }
    if ( $data_type eq 'INI' ) {
        for ( @{$data} ) {
	    next unless /^([^=]+)=(.*)/;
            $sth->execute($1,$2);
        }
    }
    if ( $data_type eq 'USR' ) {
        for ( @{$data} ) {
            my @datarow = &$parser($_);
            $sth->execute(@datarow);
        }
    }
    if ( $data_type eq 'DBI' ) {
        my $sth_old = $data->[0];
        while (my @datarow = $sth_old->fetchrow_array) {
            $sth->execute(@datarow);
        }
    }
    my $r = $DBD::RAM::ramdata;
    $r->{$table_name}->{data_type} = $data_type;
    $r->{$table_name}->{pattern}   = $pattern;
    $r->{$table_name}->{parser}    = $parser;

}

sub read_fields {
    my $dbh   = shift;
    my $str   = shift;
    my $tname = shift;
    my $type  = uc shift;
    my $catalog = $dbh->func($tname,'get_catalog');
    chomp $str;
    if ($type eq 'CSV') {
        my $sep_char = $catalog->{sep_char} || ',';
        my @fields =  Text::ParseWords::parse_line( $sep_char, 0, $str );
        return @fields;
    }
    if ($type eq 'FIX') {
	return unpack $catalog->{pattern}, $str;
    }
    if ($type eq 'INI') {
      if ( $str =~ /^([^=]+)=(.*)/ ) {
          my @fields = ($1,$2);
          return @fields;
      }
    }
    if ($type eq 'XML') {
      my @fields;
      $str =~ s#<[^>]*>([^<]*)<[^>]*>#
                my $x = $1 || '';
                push @fields, $x;
               #ge;
      return @fields;
    }
    return ();
}

sub write_fields {
    my($dbh,$fields,$tname,$type) = @_;
    my $catalog = $dbh->func($tname,'get_catalog');
    my $fieldNum =0;
    my $fieldStr = $catalog->{pattern} || '';
    $fieldStr =~ s/a//gi;
    my @fieldLengths = split / /, $fieldStr;
    $fieldStr = '';
    if( $catalog->{f_type} eq 'XML' ) {
        my @col_names = split ',',$catalog->{col_names};
        my $count =0;
        for (@col_names) {
            $fieldStr .= "<$_>$fields->[$count]</$_>";
            $count++;
	}
        return $fieldStr;
    }
    for(@$fields) {
        # PAD OR TRUNCATE DATA TO FIT WITHIN FIELD LENGTHS
        if( $catalog->{f_type} eq 'FIX' ) {
            my $oldLen = length $_;
            my $newLen =  $fieldLengths[$fieldNum];
            if ($oldLen < $newLen) { $_ = sprintf "%-${newLen}s",$_; }
  	    if ($oldLen > $newLen) { $_ = substr $_, 0, $newLen; }
            $fieldNum++;
        }
        my $newCol = $_;
        if( $catalog->{f_type} eq 'CSV' ) {
             if ($newCol =~ /,/ ) {
                 $newCol =~ s/\042/\\\042/go;
                 $newCol = qq{"$newCol"};
	     }
             $fieldStr .= $newCol . ',';
        }
        else { $fieldStr .= $newCol; 	}
        if( $catalog->{f_type} eq 'INI' ) { $fieldStr .= '='; }
    }
    if( $catalog->{f_type} eq 'CSV' ) { $fieldStr =~ s/,$//; }
    if( $catalog->{f_type} eq 'INI' ) { $fieldStr =~ s/=$//; }
    return $fieldStr;
}

sub get_music_library {
    my @dirs = @{$_[0]};
    my @db;
    for my $dir(@dirs) {
        my @files = get_music_dir( $dir );
        for my $fname(@files) {
            push @db, &get_mp3_tag($fname)
        }
    }
    return \@db;
}

sub get_music_dir {
    my $dir  = shift;
    opendir(D,$dir) || print "$dir: $!\n";
    return '' if $!;
    my @files = grep /mp3$/i, readdir D;
    @files = map ( $_ = $dir . $_, @files);
    closedir(D) || print "Couldn't read '$dir':$!";
    return @files;
}

sub get_mp3_tag {
    my($file)   = shift;
    open(I,$file) || return '';
    binmode I;
    local $/ = '';
    seek I, -128, 2;
    my $str = <I> || '';
    return '' if !($str =~ /^TAG/);
    $file = sprintf("%-255s",$file);
    $str =~ s/^TAG(.*)/$file$1/;
    my $genre = $str;
    $genre =~ s/^.*(.)$/$1/g;
    $str =~ s/(.)$//g;
    $genre = unpack( 'C', $genre );
my @genres =("Blues", "Classic Rock", "Country", "Dance", "Disco", "Funk", "Grunge", "Hip-Hop", "Jazz", "Metal", "New Age", "Oldies", "Other", "Pop", "R&B", "Rap", "Reggae", "Rock", "Techno", "Industrial", "Alternative", "Ska", "Death Metal", "Pranks", "Soundtrack", "Eurotechno", "Ambient", "Trip-Hop", "Vocal", "Jazz+Funk", "Fusion", "Trance", "Classical", "Instrumental", "Acid", "House", "Game", "Sound Clip", "Gospel", "Noise", "Alternative Rock", "Bass", "Soul", "Punk", "Space", "Meditative", "Instrumental Pop", "Instrumental Rock", "Ethnic", "Gothic", "Darkwave", "Techno-Industrial", "Electronic", "Pop-Folk", "Eurodance", "Dream", "Southern Rock", "Comedy", "Cult", "Gangsta", "Top 40", "Christian Rap", "Pop/Funk", "Jungle", "Native American", "Cabaret", "New Wave", "Psychadelic", "Rave", "Show Tunes", "Trailer", "Lo-Fi", "Tribal", "Acid Punk", "Acid Jazz", "Polka", "Retro", "Musical", "Rock & Roll", "Hard Rock", "Folk", "Folk/Rock", "National Folk", "Swing", "Fast-Fusion", "Bebop", "Latin", "Revival", "Celtic", "Bluegrass", "Avantgarde", "Gothic Rock", "Progressive Rock", "Psychedelic Rock", "Symphonic Rock", "Slow Rock", "Big Band", "Chorus", "Easy Listening", "Acoustic", "Humour", "Speech", "Chanson", "Opera", "Chamber Music", "Sonata", "Symphony", "Booty Bass", "Primus", "Porn Groove", "Satire", "Slow Jam", "Club", "Tango", "Samba", "Folklore", "Ballad", "Power Ballad", "Rhytmic Soul", "Freestyle", "Duet", "Punk Rock", "Drum Solo", "Acapella", "Euro-House", "Dance Hall", "Goa", "Drum & Bass", "Club-House", "Hardcore", "Terror", "Indie", "BritPop", "Negerpunk", "Polsk Punk", "Beat", "Christian Gangsta Rap", "Heavy Metal", "Black Metal", "Crossover", "Contemporary Christian", "Christian Rock", "Unknown");
    $genre = $genres[$genre] || '';
    $str .= $genre . "\n";
    return $str;
}


# END OF DRIVER PRIVATE METHODS

sub table_info ($) {
	my($dbh) = @_;
        my @tables;
        for (keys %{$DBD::RAM::ramdata} ) {
             push(@tables, [undef, undef, $_, "TABLE", undef]);
	}
        my $names = ['TABLE_QUALIFIER', 'TABLE_OWNER', 'TABLE_NAME',
                     'TABLE_TYPE', 'REMARKS'];
 	my $dbh2 = $dbh->{'csv_sponge_driver'};
	if (!$dbh2) {
	    $dbh2 = $dbh->{'csv_sponge_driver'} = DBI->connect("DBI:Sponge:");
	    if (!$dbh2) {
	        DBI::set_err($dbh, 1, $DBI::errstr);
		return undef;
	    }
	}

	# Temporary kludge: DBD::Sponge dies if @tables is empty. :-(
	return undef if !@tables;

	my $sth = $dbh2->prepare("TABLE_INFO", { 'rows' => \@tables,
						 'NAMES' => $names });
	if (!$sth) {
	    DBI::set_err($dbh, 1, $dbh2->errstr());
	}
	$sth;
}

sub DESTROY {undef $DBD::RAM::ramdata;}

package DBD::RAM::st; # ====== STATEMENT ======

$DBD::RAM::st::imp_data_size = 0;
@DBD::RAM::st::ISA = qw(DBD::File::st);

package DBD::RAM::Statement;

#@DBD::RAM::Statement::ISA = qw(SQL::Statement);
@DBD::RAM::Statement::ISA = qw(SQL::Statement DBD::File::Statement);
#@DBD::RAM::Statement::ISA = qw(DBD::File::Statement);

sub open_table ($$$$$) {
    my($self, $data, $tname, $createMode, $lockMode) = @_;
    my($table);
    my $dbh     = $data->{Database};
    my $catalog = $dbh->func($tname,'get_catalog');
#    if( $catalog->{f_type} && $catalog->{f_type} eq 'DBM' ) {
#    }
    if( !$catalog->{f_type} || $catalog->{f_type} eq 'RAM' ) {
        if ($createMode && !($DBD::RAM::ramdata->{$tname}) ) {
  	    if (exists($data->{$tname})) {
	        die "A table $tname already exists";
	    }
	    $table = $data->{$tname} = { 'DATA' => [],
				         'CURRENT_ROW' => 0,
				         'NAME' => $tname,
                                       };
	    bless($table, ref($self) . "::Table");
            $DBD::RAM::ramdata->{$tname} = $table;
            return $table;
        }
        else {
   	    $table = $DBD::RAM::ramdata->{$tname};
            $table->{'CURRENT_ROW'} = 0;
            return $table;
        }
    }
    else {
        my $file_name = $catalog->{f_name} || $tname;
        $table = $self->SUPER::open_table(
            $data, $file_name, $createMode, $lockMode
        );
        my @col_names;
        if ($catalog->{col_names}) {
            @col_names = split ',', $catalog->{col_names};
	}
        elsif ( !$createMode ) {
            my $fh = $table->{'fh'};
            my $line = $fh->getline || '';
            $line =~ s/[\015\012]//g;
            # READ COLUMN NAMES AS COMMA-SEPARATED LIST
            @col_names = $dbh->func($line,$tname,'CSV','read_fields');
	    # for (@col_names) {print "[$_]";} exit;
	    $table->{first_row_pos} = $fh->tell();
	}
        my $count = 0;
        my %col_nums;
        for (@col_names) { $col_nums{$_} = $count; $count++; }
        $table->{col_names} = \@col_names;
        $table->{col_nums}  = \%col_nums;
        $table->{'CURRENT_ROW'} = 0;
        $table->{NAME} = $tname;
        $table;
    }
}

package DBD::RAM::Statement::Table;

@DBD::RAM::Statement::Table::ISA = qw(DBD::RAM::Table);

package DBD::RAM::Table;

@DBD::RAM::Table::ISA = qw(SQL::Eval::Table);
#@DBD::RAM::Statement::Table::ISA = qw(SQL::Eval::Table DBD::File::Table);
#@DBD::RAM::Statement::Table::ISA = qw(DBD::File::Table);

##################################
# fetch_row()
# CALLED WITH "SELECT ... FETCH"
##################################
sub fetch_row ($$$) {
    my($self, $data, $row) = @_;
    my $dbh     = $data->{Database};
    my $tname   = $self->{NAME};
    my $catalog = $dbh->func($tname,'get_catalog');
    if( !$catalog->{f_type} || $catalog->{f_type} eq 'RAM' ) {
        my($currentRow) = $self->{'CURRENT_ROW'};
        if ($currentRow >= @{$self->{'DATA'}}) {
            return undef;
        }
        $self->{'CURRENT_ROW'} = $currentRow+1;
        $self->{'row'} = $self->{'DATA'}->[$currentRow];
        return $self->{row};
    }
    else {
        my $fields;
        if (exists($self->{cached_row})) {
  	    $fields = delete($self->{cached_row});
        } else {
	    local $/ = $catalog->{eol} || "\n";
	    #local $/ = $csv->{'eol'};
	    #$fields = $csv->getline($self->{'fh'});
	    my $fh =  $self->{'fh'} ;
            my $line = $fh->getline || return undef;
            chomp $line;
            @$fields = $dbh->func($line,$tname,$catalog->{f_type},'read_fields');
	    # @$fields = unpack $dbh->{pattern}, $line;
            if ( $dbh->{ChopBlanks} ) {
                @$fields = map($_=&trim($_),@$fields);
	    }
	    if (!$fields ) {
	      die "Error while reading file " . $self->{'file'} . ": $!" if $!;
 	      return undef;
 	    }
        }
        $self->{row} = (@$fields ? $fields : undef);
    }
    return $self->{row};
}

sub trim { my $x=shift; $x =~ s/^\s+//; $x =~ s/\s+$//; $x; }

##############################
# push_names()
# CALLED WITH "CREATE TABLE"
##############################
sub push_names ($$$) {
    my($self, $data, $names) = @_;
    my $dbh     = $data->{Database};
    my $tname   = $self->{NAME};
    my $catalog = $dbh->func($tname,'get_catalog');
    if( !$catalog->{f_type} || $catalog->{f_type} eq 'RAM' ) {
        $self->{'col_names'} = $names;
        my($colNums) = {};
        for (my $i = 0;  $i < @$names;  $i++) {
  	    $colNums->{$names->[$i]} = $i;
        }
        $self->{'col_nums'} = $colNums;
    }
    elsif(!$catalog->{col_names}) {
        # WRITE COLUMN NAMES AS CSV EVEN IF FILETYPE IS NOT CSV
        my $fh =  $self->{'fh'} ;
        my $colStr = join ',', @{$names};
        $colStr .= "\n";
        $fh->print($colStr);
    }
}

################################
# push_rows()
# CALLED WITH "INSERT" & UPDATE
################################
sub push_row ($$$) {
    my($self, $data, $fields) = @_;
    my $dbh     = $data->{Database};
    my $tname   = $self->{NAME};
    my $catalog = $dbh->func($tname,'get_catalog');
    if( !$catalog->{f_type} || $catalog->{f_type} eq 'RAM' ) {
        my($currentRow) = $self->{'CURRENT_ROW'};
        $self->{'CURRENT_ROW'} = $currentRow+1;
        $self->{'DATA'}->[$currentRow] = $fields;
        return 1;
    }
    my $fh = $self->{'fh'};
    #
    #  Remove undef from the right end of the fields, so that at least
    #  in these cases undef is returned from FetchRow
    #
    while (@$fields  &&  !defined($fields->[$#$fields])) {
	pop @$fields;
    }
    my $fieldStr = $dbh->func($fields,$tname,$catalog->{f_type},'write_fields');
    $fh->print($fieldStr,"\n");
    1;
}

sub seek ($$$$) {
    my($self, $data, $pos, $whence) = @_;
    my $dbh     = $data->{Database};
    my $tname   = $self->{NAME};
    my $catalog = $dbh->func($tname,'get_catalog');
    if( $catalog->{f_type} && $catalog->{f_type} ne 'RAM' ) {
        return DBD::File::Table::seek(
            $self, $data, $pos, $whence
        );
      }
    my($currentRow) = $self->{'CURRENT_ROW'};
    if ($whence == 0) {
	$currentRow = $pos;
    } elsif ($whence == 1) {
	$currentRow += $pos;
    } elsif ($whence == 2) {
	$currentRow = @{$self->{'DATA'}} + $pos;
    } else {
	die $self . "->seek: Illegal whence argument ($whence)";
    }
    if ($currentRow < 0) {
	die "Illegal row number: $currentRow";
    }
    $self->{'CURRENT_ROW'} = $currentRow;
}


sub drop ($$) {
    my($self, $data) = @_;
    my $dbh     = $data->{Database};
    my $tname   = $self->{NAME};
    my $catalog = $dbh->func($tname,'get_catalog');
    if( !$catalog->{f_type} || $catalog->{f_type} eq 'RAM' ) {
        my $table_name = $self->{NAME} || return;
        delete $DBD::RAM::ramdata->{$table_name}
               if $DBD::RAM::ramdata->{$table_name};
        delete $data->{$table_name}
               if $data->{$table_name};
        return 1;
    }
    return DBD::File::Table::drop( $self, $data );
}

##################################
# truncate()
# CALLED WITH "DELETE" & "UPDATE"
##################################
sub truncate ($$) {
    my($self, $data) = @_;
    my $dbh     = $data->{Database};
    my $tname   = $self->{NAME};
    my $catalog = $dbh->func($tname,'get_catalog');
    if( !$catalog->{f_type} || $catalog->{f_type} eq 'RAM' ) {
        $#{$self->{'DATA'}} = $self->{'CURRENT_ROW'} - 1;
        return 1;
    }
    return DBD::File::Table::truncate( $self, $data );
}


############################################################################
1;
__END__

=head1 NAME

 DBD::RAM - a DBI driver for in-memory data structures

=head1 SYNOPSIS

 # This sample creates a database, inserts a record, then reads
 # the record and prints it.  The output is "Hello, new world!"
 #
 use DBI;
 my $dbh = DBI->connect( 'DBI:RAM:' );
 $dbh->func( [<DATA>], 'import' );
 print $dbh->selectrow_array('SELECT col2 FROM table1');
 __END__
 1,"Hello, new world!",sample

 All syntax supported by SQL::Statement and all methods supported
 by DBD::CSV are also supported, see their documentation for 
 details.

=head1 DESCRIPTION

 DBD::RAM allows you to import almost any type of Perl data
 structure into an in-memory table and then use DBI and SQL
 to access and modify it.  It also allows direct access to
 almost any kind of flat file, supporting SQL manipulation
 of the file without converting the file out of its native
 format.

 The module allows you to prototype a database without having an
 rdbms system or other database engine and can operate either with 
 or without creating or reading disk files.

 DBD::RAM works with three differnt kinds of tables: tables
 stored only in memory, tables stored in flat files, and tables
 stored in various DBI-accessible databases.  Users may, for
 most purposes, mix and match these differnt kinds of tables
 within a script.

 Currently the following table types are supported:

    FIX   fixed-width record files
    CSV   comma separated values files
    INI   name=value .ini files
    XML   XML files (limited support)
    MP3   MP3 music files!
    RAM   in-memory tables including ones created using:
            'ARRAY' array of arrayrefs
            'HASH'  array of hashrefs
            'CSV'   array of comma-separated value strings
            'INI'   array of name=value strings
            'FIX'   array of fixed-width record strings
            'STH'   statement handle from a DBI database
            'USR'   array of user-defined data structures

 With a data type of 'USR', you can pass a pointer to a subroutine
 that parses the data, thus making the module very extendable.

=head1 WARNING

 This module is in a rapid development phase and it is likely to
 change quite often for the next few days/weeks.  I will try to keep
 the interface as stable as possible, but if you are going to start
 using this in places where it will be difficult to modify, you might
 want to ask me about the stabilitiy of the features you are using.

=head1 INSTALLATION & PREREQUISITES

 This module should work on any any platform that DBI works on.

 You don't need an external SQL engine or a running server, or a
 compiler.  All you need are Perl itself and installed versions of DBI
 and SQL::Statement. If you do *not* also have DBD::CSV installed you
 will need to either install it, or simply copy File.pm into your DBD
 directory.

 For this first release, there is no makefile, just copy RAM.pm
 into your DBD direcotry.

=head1 WORKING WITH IN-MEMORY DATABASES

=head2 CREATING TABLES

 In-memory tables may be created using standard CREATE/INSERT
 statements, or using the DBD::RAM specific import method:

    $dbh->func( $spec, $data, 'import' );

 The $spec parameter is a hashref containg:

     table_name   a string holding the name of the table
      col_names   a string with column names separated by commas
      data_type   one of: array, hash, etc. see below for full list
        pattern   a string containing an unpack pattern (fixed-width only)
         parser   a pointer to a parsing subroutine (user only)

 The $data parameter is a an arrayref containing an array of the type
 specified in the $spec{data_type} parameter holding the actual table
 data.

 Data types for the data_type parameter currently include: ARRAY, HASH,
 FIX (fixed-width), CSV, INI (name=value), DBI, and USR. See below for
 examples of each of these types.

=head3 From an array of CSV strings

  $dbh->func(
    {
      table_name => 'phrases',
      table_type => 'CSV',
      col_names  => 'id,phrase''
    }
    [
      qq{1,"Hello, new world!"},
      qq{2,"Junkity Junkity Junk"},
    ],'import' );


=head3 From an array of ARRAYS

 $dbh->func(
     {
       data_type    => 'ARRAY',
         table_name => 'phrases',
         col_names  => 'id,phrase',
     },
     [
       [1,'Hello new world!'],
       [2,'Junkity Junkity Junk'],
     ],
 'import' );

=head3 From an array of HASHES

 $dbh->func(
     { table_name => 'phrases',
       col_names  => 'id,phrase',
       data_type  => 'HASH',
     },
     [
       {id=>1,phrase=>'Hello new world!'},
       {id=>2,phrase=>'Junkity Junkity Junk'},
     ],
 'import' );

=head3 From an array of NAME=VALUE strings

 $dbh->func(
     { table_name => 'phrases',    # ARRAY OF NAME=VALUE PAIRS
       col_names  => 'id,phrase',
       data_type  => 'INI',
     },
     [
       '1=2Hello new world!',
       '2=Junkity Junkity Junk',
     ],
 'import' );

=head3 From an array of FIXED-WIDTH RECORD strings

 $dbh->func(
     { table_name => 'phrases',
       col_names  => 'id,phrase',
       data_type  => 'FIX',
       pattern    => 'a1 a20',
     },
     [
       '1Hello new world!    ',
       '2Junkity Junkity Junk',
     ],
 'import' );

 The $spec{pattern} value should be a string describing the fixed-width
 record.  See the Perl documentation on "unpack()" for details.

=head3 From directories containing MP3 MUSIC FILES

 $dbh->func(
     { data_type => 'MP3', dirs => $dirlist}, import
 )

 $dirlist should be an reference to an array of absolute paths to
 directories containing mp3 files.  Each file in those directories
 will become a record containing the fields:  file_name, song_name,
 artist, album, year, comment,genre. The fields will be filled
 in automatically from the ID3v1x header information in the mp3 file
 itself, assuming, of course, that the mp3 file contains a
 valid ID3v1x header.

=head3 From another DBI DATABASE

 You can import information from any other DBI accessible database with
 the data_type set to 'sth' in the import() method.  First connect to the
 other database via DBI and get a database handle for it separate from the
 database handle for DBD::RAM.  Then do a prepare and execute to get a
 statement handle for a SELECT statement into that database.  Then pass the
 statement handle to the DBD::RAM import() method which will perform the
 fetch and insert the fetched fields and records into the DBD::RAM table.
 After the import() statement, you can then close the database connection
 to the other database.

 Here's an example using DBD::CSV --

  my $dbh_csv = DBI->connect('DBI:CSV:','','',{RaiseError=>1});
  my $sth_csv = $dbh_csv->prepare("SELECT * FROM mytest_db");
  $sth_csv->execute();
  $dbh->func(
      { table_name => 'phrases',
        col_names  => 'id,phrase',
        data_type  => 'DBI',
      },
      [$sth_csv],
      'import'
  );
  $dbh_csv->disconnect();

=head3 From USER-DEFINED DATA STRUCTURES

 $dbh->func(
    { table_name => 'phrases',    # USER DEFINED STRUCTURE
      col_names  => 'id,phrase',
      data_type  => 'USR',
      parser     => sub { split /=/,shift },
    },
    [
        '1=Hello new world!',
        '2=Junkity Junkity Junk',
    ],
 'import' );

 This example shows a way to implement a simple name=value parser.
 The subroutine can be as complex as you like however and could, for
 example, call XML or HTML or other parsers, or do any kind of fetches
 or massaging of data (e.g. put in some LWP calls to websites as part
 of the data massaging).  [Note: the actual name=value implementation
 in the DBD uses a slightly more complex regex to be able to handle equal
 signs in the value.]

 The parsing subroutine must accept a row of data in the user-defined
 format and return it as an array.  Basically, the import() method
 will cycle through the array of data, and for each element in its
 array, it will send that element to your parser subroutine.  The
 parser subroutine should accept an element in that format and return
 an array with the elements of the array in the same order as the
 column names you specified in the import() statement.  In the example
 above, the sub accepts a string and returns and array.

 PLEASE NOTE: If you develop generally useful parser routines that others
 might also be able to use, send them to me and I can encorporate them
 into the DBD itself.

=head3 From SQL STATEMENTS

 You may also create tables with standard SQL syntax using CREATE
 TABLE and INSERT statements.  Or you can create a table with the
 import method and later populate it using INSERT statements.  Howver
 the table is created, it can be modified and accessed with all SQL
 syntax supported by SQL::Statement.

=head2 USING DEFAULTS FOR QUICK PROTOTYPING

 If no table type is supplied, an in-memory type 'RAM' will be
 assumed.  If no table_name is specified, a numbered table name
 will be supplied (table1, or if that exists table2, etc.).  The
 same also applies to column names (col1, col2, etc.).  If no
 data_type is supplied, CSV will be assumed. If the $spec parameter
 to import is missing, then defaults for all values will be used.
 Thus, the two statements below have the same effect:

    $dbh->func( [
        qq{1,"Hello, new world!"},
        qq{2,"Junkity Junkity Junk"},
        ],'import' );

    $dbh->func(
        {
            table_name => table1,
            table_type => 'CSV',
            col_names  => 'col1,col2'
        }
        [
          qq{1,"Hello, new world!"},
          qq{2,"Junkity Junkity Junk"},
        ],'import' );

=head1 WORKING WITH FLAT FILES

 This module now supports working with several different kinds of flat
 files and will soon support many more varieties.  Currently supported are
 fixed-width record files, comma separated values files, name=value ini
 files, and (with limited support) XML files.  See below for details

 To work with these kinds of files, you must first enter the table in a
 catalog specifying the table name, file name, file type, and optionally
 other information.

 Catalogs are created with the $dbh->func('catalog') command.

     $dbh->func([[
                  $table_name,
                  $table_type,
                  $file_name,
                  {optional params}
               ]])

 For example this sets up a catalog with three tables of type CSV, FIX, and
 XML:

    $dbh->func([
        ['my_csv', 'CSV', 'my_db.csv'],
        ['my_xml', 'XML', 'my_db.xml',{col_names=>'idCol,testCol'}],
        ['my_fix', 'FIX', 'my_db.fix',{pattern=>'a1 a25'}],
    ],'catalog' );

 Optional parameters include col_names -- if the column names are not
 specified with this parameter, then the module will look for the column
 names as a comma-separated list on the first line of the file.

 A table only needs to be entered into the catlog once.  After that all
 SQL statements operating on $table_name will actually be carried out on
 $file_name.  Thus, given the example catalog above, 
 "CREATE TABLE my_csv ..." will create a file called 'my_db.csv' and
 "SELECT * FROM my_xml" will open and read data from a file called
 'my_db.xml'.

 In all cases the files will be expected to be located in the directory
 named in the $dbh->{f_dir} parameter (similar to in DBD::CSV).  This
 parameter may be specified in the connect() statement as part of the
 DSN, or may be changed later with $dbh->{f_dir} = $directory, at any
 point later.  If no f_dir is specified, the current working directory
 of the script will be assumed.

=head2 CSV FILES

 This works similarly to DBD::CSV (which you may want to use instead
 if you are only working with CSV files).  It supports specifying the
 column names either in the catalog statement, or as the first line of
 the file.  It does not yet support delimiters other than commas or
 end of file characters other than newlines.

=head2 FIXED WIDTH RECORD FILES

 Column names may be specified on the first line of the file (as a
 comma separated list), or in the catalog.  A pattern must be specified
 listing the widths of the fields in the catalog.  The pattern should
 be in Perl unpack format e.g. "a2 a7 a14" would indicate a table with
 three columns with widths of 2,7,14 characters.  When data is inserted
 or updated, it will be truncated or padded to fill exactly the amount
 of space alloted to each field.

=head2 NAME=VALUE INI FILES

 Column names may be specified on the first line of the file (as a
 comma separated list), or in the catalog.

=head2 XML FILES

 Column names *must* be specified in the catalog.  Currently this module
 does not provide full support for XML files.  The feature is included
 here as a "proof of concept" experiment and will be made more robust in
 future releases.  Only a limited subset of XML is currently supported:
 files can contain tags only as specified in the catalog columns list and
 the tags must be in the same order as that list. All tags for a given
 record must occur on the same line.  The parsing routine for the tags
 is very simple minded in this release and is probably easily broken.
 In future releases, XML::Parser will be required and will replace the
 regular expression currently used.

 Here is a sample XML file that would currently work with this module:

    <name>jeff</name><state>oregon</state>
    <name>joe</name><state>new york</state>

=head1 USING MULTIPLE TABLES

 A single script can create as many tables as your RAM will support and you
 can have multiple statement handles open to the tables simultaneously. This
 allows you to simulate joins and multi-table operations by iterating over
 several statement handles at once.

=head1 TO DO

 Lots of stuff.  An export() method -- dumping the data from in-memory
 tables back into files.  More robust support for XML files.  Support for
 a variety of other easily parsed formats such as Mail files, web logs.
 Support for HTML files with the directory considered as a table, each
 HTML file considered as a record and the filename, <TITLE> tag, and
 <BODY> tags considered as fields.

 Let me know what else...

=head1 AUTHOR

 Jeff Zucker <jeff@vpservices.com>

     Copyright (c) 2000 Jeff Zucker. All rights reserved. This program is
     free software; you can redistribute it and/or modify it under the same
     terms as Perl itself as specified in the Perl README file.

     This is alpha software, no warranty of any kind is implied.

=head1 SEE ALSO

 DBI, DBD::CSV, SQL::Statement

=cut









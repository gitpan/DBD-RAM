#!/usr/local/bin/perl -w
use strict;
use DBI;
my $dbh=DBI->connect('DBI:RAM:');
#
# CHANGE THIS LIST TO YOUR DIRECTORIES CONTAINING MP3s
#
my $dirlist = ['d:/My Music/', 'c:/mp3/'];
#
$dbh->func( {data_type=>'MP3',dirs=>$dirlist},'import' );
my $sth = $dbh->prepare(q{
    SELECT song_name,genre FROM table1 WHERE year = '1999'
});
$sth->execute;
while( my($name,$genre) = $sth->fetchrow_array ) {
    printf "%25s : %s\n", $genre, $name if $name;
}
__END__

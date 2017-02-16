package AliEn::LQ::Titan;

use AliEn::LQ;
@ISA = qw( AliEn::LQ );

use IPC::Open2;
use File::Temp qw(tempfile);
use Data::Dumper;

use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my $command = join " ", @_;
  $command =~ s/"/\\"/gs;

  # my @resources = $self->get_backfill('titan', 100000);
  # $self->debug(5, Dumper(\@resources));
  #return undef if not @resources;

  my $name=$ENV{ALIEN_LOG};
  $name =~ s{\.JobAgent}{};
  $name =~ s{^(.{14}).*$}{$1};

#  $self->info("now returning");
#  return 0;

#  my $execute=$command;
#  $execute =~ s{^.*/([^/]*)$}{$ENV{HOME}/$1};

  #$execute = 'aprun -n 32 echo Hello world';

#  system ("cp",$command, $execute);
  
#  my $message = "#PBS -o $self->{PATH}/$ENV{ALIEN_LOG}.out
##PBS -e $self->{PATH}/$ENV{ALIEN_LOG}.err
##PBS -V
##PBS -N $name\n";
#  if (not $self->{NOT_STAGE_FILES}) {
#    $message.="#PBS -W stagein=$execute\@$self->{CONFIG}->{HOST}:$command\n";
#  }
#  $message.="$self->{SUBMIT_ARG}
#" . $self->excludeHosts() . "
#$execute\n";

#  $execute = "aprun -n 2560 ./get_rank_and_exec_job.py";

#  $message = "#PBS -A csc108
##PBS -l nodes=1,walltime=01:00:00
##PBS -j oe
##PBS -q titan

#cd \$MEMBERWORK/csc108
#module load cray-mpich/7.2.5
#module load python/3.4.3
#module load python_mpi4py/1.3.1
#";

my $hours =2;
my $minutes = "00";
my $nodes = 1;
my $cores = 16*$nodes;
my $max_init_wait_retries = 40;
my $max_thread_wait_retries = 40;

my $message="#!/bin/bash
#    Begin PBS directives
#PBS -A CSC108
#PBS -N jalien_multijob
#PBS -j oe
#PBS -l walltime=0$hours:$minutes:00,nodes=$nodes
#PBS -l gres=atlas1
#    End PBS directives and begin shell commands

cd \$MEMBERWORK/csc108

#module load cray-mpich/7.2.5
module load python/3.5.1
#module load python_mpi4py/1.3.1
module load python_mpi4py/2.0.0

export JOBAGENT_DB_NAME=jobagent.db
export SCRIPTS_DIR=\$PROJWORK/csc108/psvirin/ALICE_TITAN_SCRIPTS
export BUNCH_WORKDIR=\$PWD/workdir2/\$PBS_JOBID
MAX_RETRIES=$max_init_wait_retries

mkdir -p \$BUNCH_WORKDIR && cd \$_

sqlite3 \$JOBAGENT_DB_NAME \"CREATE TABLE jobagent_info (ttl INT NOT NULL, cores INT NOT NULL, started INT, max_wait_retries INT); INSERT INTO jobagent_info SELECT 8*$hours*60*60, $nodes*16, strftime( '%s', datetime('now','localtime')), $max_thread_wait_retries;\"

sleep 30

for i in `seq 1 \"\$MAX_RETRIES\"`; do
        echo Checking for database prepared
	        if [ ! -z \$(sqlite3 \$JOBAGENT_DB_NAME \"SELECT name FROM sqlite_master WHERE type='table' AND name='alien_jobs';\") ] \\
			&& [ -f \"\$JOBAGENT_DB_NAME.monitoring\" ] \\
			&& [ ! -z \$(sqlite3 \"\$JOBAGENT_DB_NAME.monitoring\" \"SELECT name FROM sqlite_master WHERE type='table' AND name='alien_jobs_monitoring'\") ]; then
				echo Databases prepared, moving on...
				break
			fi
			if [ \"\$i\" -eq \"\$MAX_RETRIES\" ]; then
				echo Retries count finished, cleaning up
				cd ..
				rm -rf \$BUNCH_WORKDIR
				exit 1
			fi
	sleep 60
done

aprun -n $cores \$SCRIPTS_DIR/get_rank_and_exec_job.py \"\$SCRIPTS_DIR\" \"\$BUNCH_WORKDIR\" \"$max_thread_wait_retries\"

cd ..
rm -rf \$BUNCH_WORKDIR";

  $self->info($message);

#$message.="\n\n$execute\n";

#"echo \"Hello\"
#echo CVMFS path: $ENV{CVMFS_PATH} 
#";

  my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
  if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
    my $dir = "";
    foreach ( split( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
        $dir .= "/$_";
        mkdir $dir, 0777;
    }
  }
 
  # writes message to tmp file
  my ($fh, $filename) = tempfile( TEMPLATE => 'titanXXXXX', DIR => $tmpdir, SUFFIX => '.pbs', UNLINK => 1);
  print $fh $message;
  
  $self->debug(1, "USING $self->{SUBMIT_CMD}\nWith filename: $filename\nWith: \n ==================  \n$message");

  my $jobid = `$self->{SUBMIT_CMD} -q titan $filename`;

  #run [ $self->{SUBMIT_CMD}, $filename ], ">", \my $jobid;
  #$self->{LAST_JOB_ID} = $jobid;

=pod
  my $pid = open2(*Reader, *Writer, "$self->{SUBMIT_CMD} " )
    or print STDERR "Can't send batch command: $!"
      and return -1;
  print Writer "$message\n";
  my $error=close Writer ;
  my $got = <Reader>;
  close Reader;
  waitpid $pid, 0;
  chomp($got);
  $got =~ /\d+.*/ and $self->{LAST_JOB_ID}=$got;
  $error or return -1;
=cut
  return 0;

}
sub getBatchId {
  my $self=shift;
  return $self->{LAST_JOB_ID};
}

sub getQueueStatus {
  my $self = shift;
  open (OUT, "$self->{GET_QUEUE_STATUS} |") or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  #    while (<OUT>) {
#	push @output, $_;
#    }

  my @output = <OUT>;
  close(OUT) or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  @output= grep ( /^\d+\S*\s+\S+.*/, @output);
  push @output,"DUMMY";
  return @output;

}


sub getStatus {
    my $self = shift;
    my $queueId = shift;
    my $refoutput = shift;
    my @output;
    my $user = getpwuid($<);
    if (!$refoutput) {
	@output = $self->getQueueStatus();
    } else {
	@output = @{$refoutput};
    }
    

    my @line = grep ( /AliEn.*/, @output );
    @line = grep ( /^$queueId/, @line );
    if ($line[0] ) {
# JobID Username Queue Jobname SessID NDS TSK Memory Time Status Time Nodes
	my @opts = split $line[0];
        if ( $opts[9] =~ /Q/ ) {
          return 'QUEUED';
        }
        if ( $opts[9] =~ /R/ ) {
          return 'RUNNING';
        }
    
	return 'QUEUED';
    }
    return 'QUEUED';
#    return 'DEQUEUED';
}

sub kill {
    my $self    = shift;
    my $queueId = shift;

    my $user = getpwuid($<);

    open( OUT, "$self->{STATUS_CMD}  |" );
    my @output = <OUT>;
    close(OUT);
    my @line = grep ( /AliEn.*/, @output );
    @line = grep ( /^$queueId/, @line );
    if ( $line[0] ) {
	$line[0] =~ /(\w*)\..*/;
        return ( system("qdel $1") );
    }
    return (2);
}

sub initialize() {
    my $self = shift;
    $self->info("Starting Titan module for AliEn");

    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "qsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "" );
    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qstat -n -1" );

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
      my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
      grep (/^alien_not_stage_files$/i, @list) and $self->{NOT_STAGE_FILES}=1;
      @list =grep (! /^alien_not_stage_files$/i, @list);
      map { $_ = "#PBS $_\n" } @list;
      $self->{SUBMIT_ARG} = "@list";
    }

    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";
    return 1;
}

sub getNumberRunning {
  my $self = shift;
  my $status = "' R '";

  $self->debug(5, "$self->{STATUS_CMD} -u ".getpwuid($<)." | grep $status |");

  if(open(OUT, "$self->{STATUS_CMD} -u ".getpwuid($<)." | grep $status | wc -l |")){
	my @output = <OUT>;
	close OUT;
	#$self->info("We have ".($#output + 1)." jobs $status");
	$self->info("We have ".($output[0])." jobs $status");
	#return $#output + 1;
	return $#output;
  }else{
	$self->info("Failed to get number of $status jobs");
  }
  return 0;
}

sub getNumberQueued {
  my $self = shift;
  my $status = "' Q '";

  $self->debug(5, "$self->{STATUS_CMD} -u ".getpwuid($<)." | grep $status |");

  if(open(OUT, "$self->{STATUS_CMD} -u ".getpwuid($<)." | grep $status | wc -l |")){
	my @output = <OUT>;
	close OUT;
	#$self->info("We have ".($#output + 1)." jobs $status");
	$self->info("We have ".($output[0])." jobs $status");
	#return $#output + 1;
	return $output[0];
  }else{
	$self->info("Failed to get number of $status jobs");
  }
  return 0;
}

sub getAllBatchIds {
  my $self=shift;
  my @output=$self->getQueueStatus() or return;

  $self->debug(1,"Checking the jobs from  @output");
  @output= grep (s/\s+.*//s, @output);

  $self->debug(1, "Returning @output");

  return @output;

}

sub get_backfill()
{
  my $self = shift;
  my ($partition, $max_nodes) = shift;
  return ((undef, undef)) if not $partition;
  
  #open(SHOWBF_PIPE, "showbf -—blocking -p $partition");
  $self->debug(5, "showbf --blocking -p $partition");
  my @output = `showbf --blocking -p $partition | tail -n +3`;
  my $i = 0;
  my $free_nodes;
  my $duration = 0;
  my @partition = ();
  my @result = ();

  $self->debug(5, Dumper(\@output));

  foreach(@output){
  	chomp;
        $self->info($_);
  	my @showbfField = split(' ',$_, 6);
  	push @partition, $showbfField[0];
  	$free_nodes = $showbfField[2];
  	my ($hours,$mins,$sec) = split(/:/,$showbfField[3]);
  	$duration = $hours * 60 + $mins * 60 + $sec;
	$self->info( "($duration, $free_nodes)" );
	push @result, ($duration, $free_nodes);
  }

  return @result;
}

return 1;



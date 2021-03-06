package AliEn::LQ::HTCONDOR;

# if you got any questions or suggestions about the module
# please contact me at: Pavlo Svirin <pavlo.svirin@cern.ch>

@ISA = qw( AliEn::LQ );

=pod
------------------
Configuration options for LDAP (Environment section):

USE_JOB_ROUTER=( 1 | 0) # whether is is necessary to use job router service
GRID_RESOURCE=condor ce504.cern.ch ce504.cern.ch:9619 	# htCondor resource for explicitly defined for submission to vanilla universe, otherwise system default resource will be selected
ROUTES_LIST=[GridResource = "condor ce504.cern.ch ce504.cern.ch:9619"; eval_set_GridResource = "condor ce504.cern.ch ce504.cern.ch:9619"; name = "Site 4"; ]  	# routes list example
USE_EXTERNAL_CLOUD=(1 | 0) # whether to use external cloud


Multiple routes example:

ROUTES_LIST = [ TargetUniverse = 5; name = "Route jobs to HTCondor"; ] [ GridResource = "batch pbs"; TargetUniverse = 9; name = "Route jobs to PBS"; ]

-------------------
Crontab script to fill the routes list from LDAP (the output file has to be readable and executable for condor user):


#!/bin/bash

echo '#!/bin/bash' > ___SOME_FILE___
echo "cat << EOF" >> ___SOME_FILE___
LDAP_ADDR=alice-ldap.cern.ch:8389
A=$(ldapsearch -x -h $LDAP_ADDR -b o=alice,dc=cern,dc=ch "(&(host=$(hostname -f))(objectClass=AlienCE))" | perl -p00e 's/\r?\n //g' | grep 'ROUTES_LIST\|USE_EXTERNAL_CLOUD')
if [ -z "$A" ]; then
        exit 3
fi

ROUTES_LIST=$(echo -n $A | sed 's/environment: /\n/g' | grep ROUTES_LIST | sed 's/ROUTES_LIST=//g')
 echo -n  $ROUTES_LIST >> ___SOME_FILE___
USE_EXTERNAL_CLOUD=$(echo $A | sed 's/environment: /\n/g' | grep USE_EXTERNAL_CLOUD | sed 's/USE_EXTERNAL_CLOUD=//')
if [ ! -z $USE_EXTERNAL_CLOUD ] && [ $USE_EXTERNAL_CLOUD -eq 1 ]; then
        echo '\\' >> ___SOME_FILE___
        echo $ROUTES_LIST | sed 's/]/set_WantExternalCloud = True; ]/g' >> ___SOME_FILE___
else
        echo >> ___SOME_FILE___
fi
echo EOF >> ___SOME_FILE___
chmod +x ___SOME_FILE___


-------------------
Cleanup script for junk files removal:

#!/bin/sh

cd ~/htcondor || exit

GZ_SIZE=10k
GZ_MINS=60
GZ_DAYS=2
RM_DAYS=7

STAMP=.stamp
prefix=cleanup-
log=$prefix`date +%y%m%d`
exec >> $log 2>&1 < /dev/null
echo === START `date`
for d in `ls -d 20??-??-??`
do
    (
        echo === $d
        stamp=$d/$STAMP
        [ -e $stamp ] || touch $stamp || exit
        if find $stamp -mtime +$RM_DAYS | grep . > /dev/null
        then
            echo removing...
            /bin/rm -r $d < /dev/null
            exit
        fi
        cd $d || exit
        find . ! -name .\* ! -name \*.gz \( -mtime +$GZ_DAYS -o \
            -size +$GZ_SIZE -mmin +$GZ_MINS \) -exec gzip -9v {} \;
    )
done
find $prefix* -mtime +$RM_DAYS -exec /bin/rm {} \;
echo === READY `date`

----------------

Crontab line for cleanup script:
37 * * * * /bin/sh /home/alicesgm/htcondor-cleanup.sh

=cut


use AliEn::LQ;
use AliEn::X509;
use AliEn::TMPFile;
use Data::Dumper;
use POSIX;
use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my ( $command, @args ) = @_;
  
  my $arglist = join " ", @args;
  $self->debug(1,"*** CONDOR.pm submit ***");

  my $error=-2;
  local $SIG{PIPE} =sub {
    $self->info("Error submitting the job: sig pipe received!\n");
    $error=-1;
  };
  $self->{X509} or $self->{X509}=AliEn::X509->new();
  $self->{X509}->checkProxy();
  $self->{COUNTER} or $self->{COUNTER}=0;
  my $cm="$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

  my @argstring=$self->{SUBMIT_ARG};

  $self->debug(1,"new status command it $self->{STATUS_CMD} \n");

#--> define temporary files for log, out & err   
  my $n=AliEn::TMPFile->new({ttl=>'24 hours', base_dir=>$self->{PATH},filename=>$ENV{ALIEN_LOG}} );

#  my $submit="executable = $command
#arguments = $arglist
#@argstring
#output = $n.out
#error  = $n.err
#log    = $n.log
#environment=CONDOR_ID=\$(Cluster).\$(Process);ALIEN_CM_AS_LDAP_PROXY=$cm;ALIEN_JOBAGENT_ID=$$.$self->{COUNTER};ALIEN_ALICE_CM_AS_LDAP_PROXY=$cm
#queue
#";
#9 ce503.cern.ch ce503.cern.ch:9619

  my $jobscript = "$self->{CONFIG}->{TMP_DIR}/agent.startup.$$";
  my $cm="$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

  my($day, $month, $year)=(localtime)[3,4,5];
  $day   = sprintf "%02d", $day;
  $month = sprintf "%02d", $month + 1;
  $year += 1900;
  my $log_folder = "$ENV{HTCONDOR_LOG_PATH}/$year-$month-$day";
  mkdir( $log_folder ) if( $ENV{HTCONDOR_LOG_PATH} and !-d $log_folder );

# ===========

  my $submit = "cmd = $jobscript\n" .
( $ENV{HTCONDOR_LOG_PATH} ?
"output = $log_folder/jobagent_$ENV{ALIEN_JOBAGENT_ID}.out
error = $log_folder/jobagent_$ENV{ALIEN_JOBAGENT_ID}.err
log = $log_folder/jobagent_$ENV{ALIEN_JOBAGENT_ID}.log\n" : "" );

# ---- direct
if(!$ENV{'USE_JOB_ROUTER'}){
	$submit .= "universe = grid
	+TransferOutput = \"\"
	#periodic_remove = (RemoteWallClockTime > 48*3600 ) || (JobStatus==5 && (CurrentTime - EnteredCurrentStatus) > 5*3600 ) 
	periodic_remove = (CurrentTime - QDate) > 7*24*3600";
	
	$submit .= "\ngrid_resource = " . $ENV{'GRID_RESOURCE'} if($ENV{'GRID_RESOURCE'});
	$submit .= "\n+WantExternalCloud = True" if($ENV{'USE_EXTERNAL_CLOUD'});
}	
else{
	$submit .= "universe = vanilla
	+WantJobRouter=True
	job_lease_duration = 7200
	should_transfer_files = YES
	periodic_remove = (CurrentTime - QDate > 7*24*3600)";
}
# ----- common
$submit .= "\nuse_x509userproxy = true
environment=\"ALIEN_CM_AS_LDAP_PROXY='$cm' ALIEN_ALICE_CM_AS_LDAP_PROXY='$cm' ALIEN_JOBAGENT_ID='$ENV{ALIEN_JOBAGENT_ID}'\"
queue 1";

# =============

  $self->{COUNTER}++;
  eval {    
    open( BATCH,"| $self->{SUBMIT_CMD}") or print  "Can't send batch command: $!" and return -2;
    $self->debug(1, "Submitting the command:\n$submit");
    print BATCH $submit;
    close BATCH or return -1;
    $error=0
  };
  if ($@) {
    $self->info("CONDOR submit command died - use debug to evaluate");
    return -2;
  }
  
  return $error;
}

# Usually, we only want the jobs that have something like 'alien'
# or 'agent' in their names (in case the same user is submitting
# some other jobs)
sub _filterOwnJobs {
  my $self     = shift;
  my @queueids = ();
  my $rcount=0;
  foreach (@_) {
    if($_ =~ /undefined/){ next; }
    if($_ =~ /HoldReason/){ next; }
    if(($_ =~ /((alien)|(agent.startup))/i)) {
       $rcount++; 
       my ($i1,$i2) = split /\ /,$_,2;
       $i1=~s/^\s*//;
       push @queueids, $i1;
    }
  }
  return @queueids;
}

# Sometimes condor is down or unresponsive leading to the appearance
# that 0 jobs are running.  If jobs=0 call this function to test health.
sub checkCondorHealth{
  my $self = shift;
  my $healthquery="condor_q";
  my $healthcheck=system($healthquery);
  if($healthcheck){
    $self->info("CONDOR healthcheck Error = $healthcheck in condor_q call, Condor might be down");
    return undef;
  }
  return 1;
}


#sub getNumberRunning{
#  my $self = shift;
##  my $jobquery="condor_q -format \"%s \" GridJobId -format \"HoldReason=%s \" HoldReasonCode -format \"%s \n\" GridJobStatus | grep -v undef | grep -v HoldReason | wc -l";
#  my $jobquery="condor_q -constraint \"JobStatus==1 || JobStatus==2\" | wc -l";
#  open(JOBS,"$jobquery |") or $self->info("error doing $jobquery");
#  my $njobs=<JOBS>;
#  close(JOBS);
#  $njobs=~s/\n//;
#  if($njobs == 0 && (!defined $self->checkCondorHealth())){
#    $self->info("Error getting total number of jobs, unable to check condor queue");
#    return undef;
#  }
#  return $njobs;
#}
#
#sub getNumberQueued{
#  my $self = shift;
##  my $jobquery="condor_q -format \"%s \" GridJobId -format \"%s \n\" GridJobStatus | grep PENDING | wc -l";
##  my $jobquery="condor_q -format \"%s \" GridJobId -format \"HoldReason=%s \" HoldReasonCode -format \"%s \n\" GridJobStatus | grep PENDING | grep -v HoldReason | wc -l";
#  my $jobquery="condor_q -constraint \"JobStatus==1\" | wc -l";
#
#  open(JOBS,"$jobquery |") or $self->info("error doing $jobquery");
#  my $njobs=<JOBS>;
#  close(JOBS);
#  $njobs=~s/\n//;
#  if($njobs == 0 && (!defined $self->checkCondorHealth())){
#     $self->info("Error getting number of queued jobs, unable to check condor queue");
#     return undef;
#  } 
#  return $njobs;
#} 

sub getNumberRunning{
  my $self = shift;
  my $totals = `condor_q $self->{USERNAME} -totals | tail -n 1`;
  return undef if $?;
  my $running = undef;
  $totals =~ /(\d+) running/;
  $running = $1;
  return $running;
}

sub getNumberQueued{
  my $self = shift;
  my $totals = `condor_q $self->{USERNAME} -totals | tail -n 1`;
  return undef if $?;
  my $queued = 0;
  $totals =~ /(\d+) idle,\s+(\d+) running,\s+(\d+) held,\s+(\d+) suspended/;
  $queued = $1 + $3 + $4;
  return $queued;
}


sub getStatus {
  return 'QUEUED';
}

sub initialize() {
  my $self = shift;

  $self->{PATH} = $self->{CONFIG}->{LOG_DIR};
  $self->{X509}=AliEn::X509->new();


  $self->debug(1,"In CONDOR.pm initialize");
  $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "condor_submit" );
  $self->{SUBMIT_ARG}="";
  
  $self->{USERNAME} = $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);

  if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
    my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
    foreach (@list) {
      $self->debug(1,"CE::CONFIG Submit arg --> $_");
      my $entry=$_;
        if($entry=~m/grid_resource/){
          $entry=~s/\=/\=gt2 /;
        }
	if($entry=~m/GlobusScheduler/){  # old style ... backwords compatible
	    my @tmp=split("=",$entry);
            $entry="grid_resource=gt2 ".$tmp[1];
	}
        
      $self->{SUBMIT_ARG}.="$entry\n";
    }
  }
  $self->{KILL_CMD} = ( $self->{CONFIG}->{CE_KILLCMD} or "condor_rm" );
#  $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "condor_q -format \"%d \" ClusterId -format \"HoldReason=%s \" HoldReasonCode -format \"%s \" GridJobId -format \"%s \\n\" Cmd" );
  $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "condor_q -constraint \"JobStatus==1 || JobStatus==2\" -format \"%d \" ClusterId -format \"%s \\n\" Cmd");
  $self->debug(1,"CONDOR SUBMIT ARG = $self->{SUBMIT_ARG}");
  $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD}";
  if ( $self->{CONFIG}->{CE_STATUSARG} ) {
    $self->{GET_QUEUE_STATUS}.=" @{$self->{CONFIG}->{CE_STATUSARG_LIST}}"
  }

  #%{$self->{POSSIBLE_CES}} = {"ce503.cern.ch" => "ce503.cern.ch:9613", "alicondorce01.cern.ch" => "alicondorce01.cern.ch:9619" };

  $self->debug(1,"CONDOR intialize finished");
  return 1;
}

sub kill {
    my $self    = shift;
    my $queueId = shift;
    my ( $id, @rest ) = split ( ' ', `$self->{STATUS_CMD} | grep $queueId\$` );
    $id or $self->info("Command $queueId not found in condor!!") and return -1;
    print STDERR "In CONDOR, killing process $queueId (id $id)\n";
    return ( system(" $self->{KILL_CMD} $id") );
}


sub removeKilledProcesses{
  my $self=shift;
  my @jobs=();
  foreach my $line (@_){
    my ($id, $user,$date, $time, $cpu, $status, $rest)=split (/\s+/, $line);
    ($status  and  ($status eq "X"))
      or  push @jobs, $line;
  }
  return @jobs;
}


sub ping_ce{
  my ($self, $ce_name, $ce_pool) = shift;
  return 2 if !$ce_name || !$ce_pool;
  return system("condor_ping -verbose -name ce_name -pool $ce_pool WRITE");
}


return 1;

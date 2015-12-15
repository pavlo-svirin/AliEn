package AliEn::LQ::PANDA;
use AliEn::LQ;
use AliEn::Config;
@ISA = qw( AliEn::LQ);
use strict;

use AliEn::Database::CE;
use File::Basename;
use File::Copy;

use Data::Dumper;
# use lib '/usr/bin/perl';
# use Inline::Python;

#use Inline;

sub initialize{
  my $self=shift;

}

#my $job = "from taskbuffer.JobSpec import JobSpec";
#Inline->bind( Python => $job );



sub submit {
     	my $self = shift;
	my $classad = shift;
        my $script = shift;

        open FILE,">",$script;
        print FILE "#!/bin/bash\nhostname -f\ndate";
        close FILE;

        my $content = `cat $script`;

        $self->info("Script: $content");

        my $result=`python /home/alienmaster/titan.py $script`;
	#my $site = "ANALY_ORNL_Titan";
	#$job->{ComputingSite}= $site;    
	#$job->{transformation}="/alice/cern.ch/user/p/psvirin/bin/sleep.sh";
	#$job->{VO}="alice";
	#$job->{prodSourceLabel}="panda";
	#use Inline Python=>'import from userinterface.Client as Client';
	#Inline->bind( Python=>'import from userinterface.Client as Client');
	#my $Client = "import from userinterface.Client as Client";
	#Inline->bind( Python => $Client);
	#my $aSrvID=undef;
	#my $s=$Client->{submitJobs($job,$aSrvID)};
	#$s and $self->info("PanDA ID : ".Dumper($s) );
        $result and $self->info( "We got: ".Dumper($result) );
        return 0;
}

sub kill {
        my $self = shift;
	my $killjob=`python killJob.py`
}
return 1;


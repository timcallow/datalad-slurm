#!/usr/bin/env bash

set +e # continue on errors

# Test datalad 'reschedule' 
#   - create some job dirs and job scripts and 'commit' them
#   - then 'datalad schedule' all jobs from their job dirs
#   - wait until all of them are finished, then run 'datalad reschedule --since='
#   - then run 'datalad reschedule --since=' again
#   - wait until all reschedule jobs are finished and then run 'datalad finish'
#
# Expected results: The first 'datalad reschedule' should run without errors;
# the second should produce an error because of output conflicts

if [[ -z $1 ]] ; then

    echo "no temporary directory for tests given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit -1
fi

D=$1

echo "start"

B=`dirname $0`

echo "from src dir "$B

## create a test repo

TESTDIR=$D/"datalad-slurm-test-09_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###

# make the slurm batch script

if [ ! -f "slurm_config.txt" ]; then
    echo "Error: slurm_config.txt must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit -1
fi

source slurm_config.txt

# Create the script
cat << EOF > $TESTDIR/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest09"         # name of the job
#SBATCH --partition=$partition        
#SBATCH -A $account
#SBATCH --time=0:05:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
OUTPUT="output_test_"\`date -Is|tr -d ":"\`.txt
# simulate some text output
for i in \`seq 1 50\`; do
    echo \$i | tee -a \$OUTPUT
    sleep 1s
done
# simulate some binary output which will become an annex file
bzip2 -k \$OUTPUT
echo "ended"
EOF

# Make the script executable
chmod u+x $TESTDIR/slurm.template.sh

cd $TESTDIR

TARGETS=`seq 0 2`

for i in $TARGETS ; do

    DIR="test_09_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh

done

datalad save -m "add test job dirs and scripts"

for i in $TARGETS ; do

    DIR="test_09_output_dir_"$i

    cd $DIR
    datalad schedule -o $PWD sbatch slurm.sh
    cd ..

done

while [[ 0 != `squeue -u $USER | grep "DLtest09" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad finish --list-open-jobs

echo "finishing completed jobs:"
datalad finish

# now we reschedule the jobs (should work without issues)
echo "rescheduling all jobs:"
datalad reschedule --since=

# now reschedule again - should fail due to conflicts
echo "rescheduling jobs with conflicts (should fail):"
datalad reschedule --since=

while [[ 0 != `squeue -u $USER | grep "DLtest09" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad finish --list-open-jobs

# close up rescheduled jobs
echo "finishing rescheduled jobs"
datalad finish --close-failed-jobs


#echo " ### git log in this repo ### "
#echo ""
#git log




#!/usr/bin/env bash

set +e # do NOT abort on errors

# Test datalad 'schedule' and 'finish' functionality
#   - 'datalad schedule' several jobs with the same output dir but different output file names
#   - then 'datalad schedule' more jobs from the same set of job dirs
#   - wait until all of them are finished, then run 'datalad finish'
#
# Expected results: should handle the first set of jobs fine until the end, 
# but refuse to schedule the second set of jobs

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

TESTDIR=$D/"datalad-slurm-test-07_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###
if [ ! -f "slurm_config.txt" ]; then
    echo "Error: slurm_config.txt must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit -1
fi

source slurm_config.txt

# Create the script
cat <<EOF > $TESTDIR/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest07"         # name of the job
#SBATCH --partition=$partition       
#SBATCH -A $account
#SBATCH --time=0:05:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out

echo "started"

OUTPUT=\$1
if [ -z \$OUTPUT ]; then
    echo "no OUTPUT FILE given as argument, abort"
    exit -1
fi

# simulate some text output
for i in \$(seq 1 50); do
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
MAINDIR=$PWD

DIR="test_04_output_dir_for_all"
mkdir -p $DIR

# First we schedule jobs which don't conflict
echo "    --> try to schedule permitted jobs"

SUBDIRS1=($DIR"/OUTPUT1/" $DIR"/OUTPUT2/" $DIR"/OUTPUT3a/OUTPUT3b/")

for SUBDIR in "${SUBDIRS1[@]}"; do
    cd $MAINDIR
    mkdir -p $SUBDIR

    cp slurm.template.sh $SUBDIR/slurm.sh

    datalad save -m "add test job dir and script"

    cd $SUBDIR

    OUTPUTFILENAME="test_07_output_file"

    echo datalad schedule -o $PWD sbatch slurm.sh $OUTPUTFILENAME
    datalad schedule -o $PWD sbatch slurm.sh $OUTPUTFILENAME

done

sleep 5s

echo "    --> now try to schedule conflicting jobs"

SUBDIRS2=($DIR"/OUTPUT1/" $DIR"/OUTPUT2/OUTPUT2b/" $DIR"/OUTPUT3a/")

for SUBDIR in "${SUBDIRS2[@]}"; do
    cd $MAINDIR
    mkdir -p $SUBDIR

    cp slurm.template.sh $SUBDIR/slurm.sh

    datalad save -m "add test job dir and script"

    OUTPUTFILENAME="test_07_output_file"

    echo datalad schedule -o $PWD sbatch slurm.sh $OUTPUTFILENAME
    datalad schedule -o $PWD sbatch slurm.sh $OUTPUTFILENAME

    cd $SUBDIR

done

cd $MAINDIR

while [[ 0 != `squeue -u $USER | grep "DLtest07" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad finish --list-open-jobs

echo "finishing completed jobs:"
datalad finish

echo "closing failed jobs:"
datalad finish --close-failed-jobs

#echo " ### git log in this repo ### "
#echo ""
#git log




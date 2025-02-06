#!/usr/bin/env bash

set +e # do NOT abort on errors

# Test datalad 'schedule' and 'finish' functionality
#   - create some job dirs and job scripts and 'commit' them
#   - then 'datalad schedule' all jobs from their job dirs
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

TESTDIR=$D/"datalad-slurm-test-02_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###


cp $B/slurm_test01.template.sh $TESTDIR/slurm.template.sh
cd $TESTDIR

TARGETS=`seq 17 21`

for i in $TARGETS ; do

    DIR="test_02_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm1.sh
    cp slurm.template.sh $DIR/slurm2.sh

done

datalad save -m "add test job dirs and scripts"

echo "    --> schedule some jobs"

for i in $TARGETS ; do

    DIR="test_02_output_dir_"$i

    cd $DIR
    datalad schedule -o $PWD sbatch slurm1.sh
    cd ..

done

sleep 5s

echo "    --> now try to schedule conflicting jobs"

for i in $TARGETS ; do

    DIR="test_02_output_dir_"$i

    cd $DIR
    datalad schedule -o $PWD sbatch slurm2.sh
    cd ..

done


while [[ 0 != `squeue -u $USER | grep "DLtest01" | wc -l` ]] ; do

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




#!/usr/bin/env bash

set +e # continue on errors

# Test how datalad 'schedule' and 'finish' handle failed jobs
#   - create some job dirs and job scripts and 'commit' them
#   - then 'datalad schedule' all jobs from their job dirs
#   - some of the jobs will fail (also feel free to `scancel some`)
#   - wait until all of them are finished, then run 'datalad finish'
#   - check if the remaining jobs will be shown correctly
#   - check if the remaining jobs are correctly closed
#
# Expected results: should run without any errors

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

TESTDIR=$D/"datalad-slurm-test-06_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###


cp $B/slurm_test06.template.sh $TESTDIR/slurm.template.sh
cd $TESTDIR

TARGETS=`seq 41 42`

for i in $TARGETS ; do

    DIR="test_06_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh

done

datalad save -m "add test job dirs and scripts"

for i in $TARGETS ; do

    DIR="test_06_output_dir_"$i
    FILE="output_test_array_*.txt"

    cd $DIR
    echo datalad schedule -o $PWD/$FILE --allow-wildcard-outputs sbatch slurm.sh $i
    datalad schedule -o $PWD/$FILE --allow-wildcard-outputs sbatch slurm.sh $i
    cd ..

done

while [[ 0 != `squeue -u $USER | grep "DLtest06" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

echo -e "\n #### Open jobs before 'datalad finish':\n"
datalad finish --list-open-jobs

echo -e "finishing completed jobs:"
datalad finish

echo -e \n" #### Open jobs after 'datalad finish':\n"
datalad finish --list-open-jobs


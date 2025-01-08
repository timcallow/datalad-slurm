#!/usr/bin/env bash

set -e # abort on errors

# Test datalad 'schedule' and 'finish' functionality
#   - 'datalad schedule' several jobs with the same output dir but different output file names
#   - wait until all of them are finished, then run 'datalad finish'
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

TESTDIR=$D/"datalad-slurm-test-03_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###


cp $B/slurm_test03.template.sh $TESTDIR/slurm.template.sh
cd $TESTDIR

TARGETS=`seq 29 33`

DIR="test_03_output_dir_for_all"
mkdir -p $DIR
cp slurm.template.sh $DIR/slurm.sh

datalad save -m "add test job dir and script"

cd $DIR

for i in $TARGETS ; do

    OUTPUTFILENAME="test_03_output_file_"$i

    datalad schedule -o $PWD/$OUTPUTFILENAME sbatch slurm.sh $OUTPUTFILENAME

done

cd ..

while [[ 0 != `squeue -u $USER | grep "DLtest03" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad finish --list-open-jobs

echo "finishing completed jobs:"
datalad finish

echo " ### git log in this repo ### "
echo ""
git log




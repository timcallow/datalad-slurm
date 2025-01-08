#!/usr/bin/env bash

set -e # abort on errors

# Test datalad 'schedule' and 'finish' functionality
#   - create some job dirs and job scripts and 'commit' them, this time with wildcards in the dir names
#   - then 'datalad schedule' all jobs from their job dirs
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

TESTDIR=$D/"datalad-slurm-test-01_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###


cp $B/slurm_test01.template.sh $TESTDIR/slurm.template.sh
cd $TESTDIR

TARGETS=`seq 17 21`

for i in $TARGETS ; do

    DIR="test_01_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh

done

datalad save -m "add test job dirs and scripts"

for i in $TARGETS ; do

    DIR="test_01_output_dir_"$i

    cd $DIR
    datalad schedule -o "test_*_output_dir_"$i sbatch slurm.sh
    cd ..

done

while [[ 0 != `squeue -u $USER | grep "DLtest01" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad finish --list-open-jobs

echo "finishing completed jobs:"
datalad finish

echo " ### git log in this repo ### "
echo ""
git log
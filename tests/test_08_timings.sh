#!/usr/bin/env bash

set -e # abort on errors

# Test datalad 'schedule' and 'finish --list-open-jobs' and 'finish' functionality
#   - measure how long they take with growing git log length
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

TARGETS=`seq 1 1000`

for i in $TARGETS ; do

    DIR="test_01_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh

done

datalad save -m "add test job dirs and scripts"



for i in $TARGETS ; do

    DIR="test_01_output_dir_"$i

    cd $DIR
    /usr/bin/time -f "%E real" -o ../timing_schedule.txt -a datalad schedule -o $PWD sbatch slurm.sh
    cd ..

    sleep 1s

    ## this produces an error sometimes
    ## [ERROR  ] Error on 73ff5ea92ef661478bd9d54641e7b3873b66a47c's message -caused by- Job 9283024 not found 
    #/usr/bin/time -f "%E real" -o timing_finish-list.txt -a datalad finish --list-open-jobs

done

while [[ 0 != `squeue -u $USER | grep "DLtest01" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

echo "done waiting"

/usr/bin/time -f "%E real" -o timing_finish-list.txt -a datalad finish --list-open-jobs

echo "finishing completed jobs:"
/usr/bin/time -f "%E real" -o timing_finish.txt -a datalad finish

#echo " ### git log in this repo ### "
#echo ""
#git log




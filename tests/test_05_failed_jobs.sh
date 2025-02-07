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

TESTDIR=$D/"datalad-slurm-test-05_"`date -Is|tr -d ":"`

datalad create -c text2git $TESTDIR


### generic part for all the tests ending here, specific parts follow ###

if [ ! -f "slurm_config.txt" ]; then
    echo "Error: slurm_config.txt must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit -1
fi

source slurm_config.txt

cat << EOF > $TESTDIR/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest05"         # name of the job
#SBATCH --partition=$partition       
#SBATCH -A $account
#SBATCH --time=0:05:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
ARGUMENT=\$1
OUTPUT="output_test_"\$(date -Is|tr -d ":").txt
# simulate some text output
for i in \$(seq 1 50); do
    echo \$i | tee -a \$OUTPUT
    sleep 1s
    if (( i == 20 )); then
        if (( ARGUMENT % 2 == 0 )); then
            echo "abort"
            exit -12
        fi
    fi
done
# simulate some binary output which will become an annex file
bzip2 -k \$OUTPUT
echo "ended"
EOF

chmod u+x $TESTDIR/slurm.template.sh

cd $TESTDIR

TARGETS=`seq 35 39`

for i in $TARGETS ; do

    DIR="test_05_output_dir_"$i
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh

done

datalad save -m "add test job dirs and scripts"

for i in $TARGETS ; do

    DIR="test_05_output_dir_"$i

    cd $DIR
    datalad schedule -o $PWD sbatch slurm.sh $i
    cd ..

done

while [[ 0 != `squeue -u $USER | grep "DLtest05" | wc -l` ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

echo -e "\n #### Open jobs before 'datalad finish':\n"
datalad finish --list-open-jobs

echo -e "finishing completed jobs:"
datalad finish

echo -e \n" #### Open jobs after 'datalad finish':\n"
datalad finish --list-open-jobs

echo "closing failed jobs :"
datalad finish --close-failed-jobs

echo -e "\n #### Open jobs after 'datalad finish --close-failed-jobs':\n"
datalad finish --list-open-jobs



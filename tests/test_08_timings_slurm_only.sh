#!/usr/bin/env bash

set +e # continue on errors

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

TESTDIR=$D/"datalad-slurm-test-08_"`date -Is|tr -d ":"`

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
#SBATCH --job-name="DLtest08"         # name of the job
#SBATCH --partition=defq              # partition to be used (defq, gpu or intel)
#SBATCH -A casus
#SBATCH --time=0:02:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
OUTPUT="output_test_"\$(date -Is|tr -d ":").txt
# simulate some text output
for i in \$(seq 1 20); do
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

TARGETS=`seq 1 10000`

echo "Create job scripts:"

for i in $TARGETS ; do

    M=$(($i%100))
    DIR="$M/test_08_output_dir_$i"
    mkdir -p $DIR

    cp slurm.template.sh $DIR/slurm.sh

done

datalad save -m "add test job dirs and scripts"

echo "Schedule jobs:"
echo "num_jobs time">timing_slurm.txt
for i in $TARGETS ; do

    M=$(($i%100))
    DIR="$M/test_08_output_dir_$i"

    echo -n $i" ">>timing_schedule.txt
    /usr/bin/time -f "%e" -o timing_slurm.txt -a sbatch --chdir $DIR slurm.sh

    sleep 1s

    if [[ 0 == $M ]]; then
        while [[ $LIMITJOBS < `squeue -u $USER | grep "DLtest08" | wc -l` ]] ; do

            echo "    ... wait for jobs to finish inbetween"
            sleep 30s
        done
    fi

done

scancel -n "DLtest08"

echo "done waiting" 




#!/usr/bin/env bash

set -e # abort on errors

# Test datalad 'slurm-schedule' and 'slurm-finish' functionality
#   - 'datalad slurm-schedule' several jobs with the same output dir but different output file names
#   - then 'datalad slurm-schedule' more jobs from the same set of job dirs
#   - wait until all of them are finished, then run 'datalad slurm-finish'
#
# Expected results: should handle the first set of jobs fine until the end, 
# but refuse to schedule the second set of jobs

if [[ -z $1 ]] ; then

    echo "no temporary directory for tests given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit 1
fi

D=$1

echo "start"

B=$(dirname "$0")

echo "from src dir ""$B"

## create a test repo

TESTDIR=$D/"datalad-slurm-test-04_"$(date -Is|tr -d ":")

datalad create -c text2git "$TESTDIR"


### generic part for all the tests ending here, specific parts follow ###

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Using script dir: $SCRIPT_DIR"

CONFIG_FILE="$SCRIPT_DIR/slurm_config.txt"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: $CONFIG_FILE must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit 1
fi

. "$CONFIG_FILE"

# Create the script
cat <<EOF > "$TESTDIR"/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest04"         # name of the job
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
chmod u+x "$TESTDIR"/slurm.template.sh

cd "$TESTDIR"

TARGETS=$(seq 29 33)

DIR="test_04_output_dir_for_all"
mkdir -p $DIR
cp slurm.template.sh $DIR/slurm.sh

datalad save -m "add test job dir and script"

cd "$DIR"
    
for i in $TARGETS ; do

    OUTPUTFILENAME="test_04_output_file_"$i

    echo datalad slurm-schedule -o "$PWD"/"$OUTPUTFILENAME" sbatch slurm.sh "$OUTPUTFILENAME"
    datalad slurm-schedule -o "$PWD"/"$OUTPUTFILENAME" sbatch slurm.sh "$OUTPUTFILENAME"

done

cd ..

sleep 5s

echo "    --> now try to schedule conflicting jobs"

cd "$DIR"

SCHED_FAILED=0

for i in $TARGETS ; do

    OUTPUTFILENAME="test_04_output_file_"$i

    echo datalad slurm-schedule -o "$PWD"/"$OUTPUTFILENAME" sbatch slurm.sh "$OUTPUTFILENAME"
    if ! datalad slurm-schedule -o "$PWD"/"$OUTPUTFILENAME" sbatch slurm.sh "$OUTPUTFILENAME"
    then
        SCHED_FAILED=1
        echo "Expected failure: conflicting job in $DIR"
    fi

done

cd ..


while [[ 0 != $(squeue -u "$USER" | grep "DLtest04" | wc -l) ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 1m
done

datalad slurm-finish --list-open-jobs

echo "finishing completed jobs:"
datalad slurm-finish

echo "closing failed jobs:"
datalad slurm-finish --close-failed-jobs

if [[ $SCHED_FAILED -eq 1 ]]; then
    echo "Test succeeded: conflict detected"
    exit 0
else
    echo "Test FAILED: no conflicts detected but should have"
fi
    exit 1

#echo " ### git log in this repo ### "
#echo ""
#git log




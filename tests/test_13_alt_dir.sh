#!/usr/bin/env bash

set -e # abort on errors

# Test datalad 'slurm-schedule' and 'slurm-finish' functionality
#   - create some job dirs and job scripts and 'commit' them
#   - then 'datalad slurm-schedule' all jobs from their job dirs
#   - wait until all of them are finished, then run 'datalad slurm-finish'
#
# Expected results: should run without any errors

if [[ -z $1 ]] ; then

    echo "no temporary directory for tests given, abort"
    echo ""
    echo "... call as $0 <dir>"

    exit 1
fi

D=$1

if [[ -z $2 ]] ; then

    echo "no alternative directory for test repository given, abort"
    echo ""
    echo "... call as $0 <test-dir> <alt-dir>"

    exit 1
fi

A=$2

echo "repository in $D and alternative directory in $A"
A1=$A/foo/
A2=$A/bar/
mkdir -p "$A1" "$A2"

## create a test repo

TESTDIR=$D/"datalad-slurm-test-13_"$(date -Is|tr -d ":")
datalad create -c text2git "$TESTDIR"


### generic part for all the tests ending here, specific parts follow ###

# make the slurm batch script

echo "Using script dir: $SCRIPT_DIR"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CONFIG_FILE="$SCRIPT_DIR/slurm_config.txt"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: $CONFIG_FILE must exist"
    echo "Please see slurm_config_sample.txt for a template"
    exit 1
fi

. "$CONFIG_FILE"

# Create the script
cat << EOF > "$TESTDIR"/slurm.template.sh
#!/bin/bash
#SBATCH --job-name="DLtest13"         # name of the job
#SBATCH --partition=$partition        
#SBATCH -A $account
#SBATCH --time=0:05:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out
echo "started"
OUTPUT="output_test.txt"
# simulate some text output
for i in \`seq 1 10\`; do
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

## schedule and run 2 jobs in the repository

TARGETS=$(seq 17 18)

for i in $TARGETS ; do

    DIR="test_13_output_dir_"$i
    mkdir -p "$DIR"

    cp slurm.template.sh "$DIR"/slurm.sh
    echo "aaa">"$DIR"/aaa.txt
    mkdir -p "$DIR"/bbb
    echo "ccc">"$DIR"/bbb/ccc.txt
    mkdir -p "$DIR"/ddd/eee
    echo "fff">"$DIR"/ddd/eee/fff.txt
    mkdir -p "$DIR"/ggg/hhh/iii/
    echo "jjj">"$DIR"/ggg/hhh/iii/jjj.txt
    mkdir -p "$DIR"/kkk/lll/mmm/
    echo "nnn">"$DIR"/kkk/lll/mmm/nnn.txt

done

datalad save -m "add test job dirs and scripts"

for i in $TARGETS ; do

    DIR="test_13_output_dir_"$i

    cd "$DIR"
    datalad slurm-schedule -i slurm.sh -i aaa.txt -i bbb/ccc.txt -i ddd/eee/fff.txt -i ggg/hhh/iii -i kkk/lll/mmm/ -o output_test.txt -o output_test.txt.bz2 --alt-dir "$A1" sbatch slurm.sh
    cd ..

done
# sleep infinity 
sleep 3s
while [[ 0 != $(squeue -u "$USER" | grep "DLtest13" | wc -l) ]] ; do

    echo "    ... wait for jobs to finish"
    sleep 30s
done

datalad slurm-finish --list-open-jobs
echo ""

echo "finishing completed jobs:"
echo "    ""$PWD"
datalad slurm-finish

sleep 3s

echo "    STATUS"
datalad status

## now reschedule the jobs

# reschedule the later job with an different --alt-dir
COMMITID=$(git log -2 --oneline|head -1|awk '{print $1}')
datalad slurm-reschedule "$COMMITID" --alt-dir "$A2"

# reschedule the earlier of the two jobs in-place, that means without a --alt-dir
COMMITID=$(git log -2 --oneline|tail -1|awk '{print $1}')
datalad slurm-reschedule "$COMMITID"

sleep 3s
while [[ 0 != $(squeue -u "$USER" | grep "DLtest13" | wc -l) ]] ; do

    echo "    ... wait for rescheduled jobs to finish"
    sleep 1m
done

echo "    STATUS"
datalad status

datalad slurm-finish --list-open-jobs
echo ""

echo "finishing completed jobs:"
echo "    ""$PWD"
datalad slurm-finish



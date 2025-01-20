#!/bin/bash
#SBATCH --job-name="DLtest03"         # name of the job
#SBATCH --partition=casus_genoa       # partition to be used (defq, gpu or intel)
#SBATCH -A casus
#SBATCH --time=0:05:00                # walltime (up to 96 hours)
#SBATCH --ntasks=1                    # number of nodes
#SBATCH --cpus-per-task=1             # number of tasks per node
#SBATCH --output=log.slurm-%j.out


echo "started"

OUTPUT=$1
if [ -z $OUTPUT ]; then
    echo "no OUTPUT FILE given as argument, abort"
    exit -1
fi

# simulate some text output
for i in `seq 1 50`; do

    echo $i | tee -a $OUTPUT
    sleep 1s
done

# simulate some binary output which will become an annex file
bzip2 -k $OUTPUT

echo "ended"

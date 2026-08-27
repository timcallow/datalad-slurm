#!/bin/sh
set -e
ds=/data/test-multiple-jobs
if [ ! -d "$ds/.git" ]; then
  datalad create -c text2git "$ds"
fi
cd "$ds"

# Hello world job script
cat > job1.sh <<'EOF'
#!/bin/bash
#SBATCH --job-name=hello
#SBATCH --output=slurm-1.out
echo "Hello world" >> slurm-1.out
EOF

cat > job2.sh <<'EOF'
#!/bin/bash
#SBATCH --job-name=hello
#SBATCH --output=slurm-2.out
echo "Hello world" >> slurm-2.out
EOF
chmod +x job*.sh

datalad save -m "Add job scripts"


# sleep infinity & wait

# Schedule via the DataLad extension wrapping sbatch
datalad slurm-schedule -o slurm-1.out sbatch ./job1.sh
datalad slurm-schedule -o slurm-2.out sbatch ./job2.sh


# Poll until the job is no longer in the queue
while squeue -h | grep -q .; do
  sleep 2
done

# Capture open job list
output=$(datalad slurm-finish --list-open-jobs)

echo "$output"
completed_count=$(echo "$output" | grep -Ec '^[[:space:]]*[0-9]+\s+COMPLETED')

# Verify that there are exactly two COMPLETED jobs
if [ "$completed_count" -eq 2 ]; then
  echo "Both jobs completed successfully!"
else
  echo "Unexpected output: $output"
  exit 1
fi
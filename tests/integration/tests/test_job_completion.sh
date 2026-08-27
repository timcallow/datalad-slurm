#!/bin/sh
set -e
ds=/data/test-job-completion
if [ ! -d "$ds/.git" ]; then
  datalad create -c text2git "$ds"
fi
cd "$ds"

# Hello world job script
cat > job.sh <<'EOF'
#!/bin/bash
#SBATCH --job-name=hello
#SBATCH --output=slurm-1.out
echo "Hello world" >> slurm-1.out
EOF
chmod +x job.sh
datalad save -m "Add job script"

# sleep infinity & wait

# Schedule via the DataLad extension wrapping sbatch
datalad slurm-schedule -o slurm-1.out sbatch ./job.sh


# Poll until the job is no longer in the queue
while squeue -h | grep -q .; do
  sleep 2
done

# Capture open job list
output=$(datalad slurm-finish --list-open-jobs)

echo "$output"

# Verify that there is exactly one COMPLETED job
if echo "$output" | grep -Eq '^[[:space:]]*[0-9]+\s+COMPLETED'; then
  echo "Job completed successfully!"
else
  echo "Unexpected job status: $output"
  exit 1
fi
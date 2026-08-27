#!/bin/sh
set -e
ds=/data/test-job-cancel
if [ ! -d "$ds/.git" ]; then
  datalad create -c text2git "$ds"
fi
cd "$ds"

# Hello world job script
cat > job.sh <<'EOF'
#!/bin/bash
#SBATCH --job-name=job_to_cancel
#SBATCH --output=slurm.out
sleep 60
echo "Hello world" >> slurm.out
EOF

chmod +x job.sh

datalad save -m "Add job scripts"


# sleep infinity & wait

# Schedule via the DataLad extension wrapping sbatch
datalad slurm-schedule -o slurm.out sbatch ./job.sh

echo "Waiting for job to appear in job list..."
# Poll until job appears (important)
while true; do
    job_id=$(
        datalad slurm-finish --list-open-jobs \
        | awk ' /^[[:space:]]*[0-9]+/ {print $1}' \
        | tail -n 1
    )
    if [ -n "$job_id" ]; then
        break
    fi
  sleep 1
done

echo "Job submitted with ID: $job_id"

echo "Cancelling job..."
scancel "$job_id"

echo "Waiting for job to leave the queue..."
while squeue -h | grep -q "$job_id"; do
  sleep 1
done



output=$(datalad slurm-finish --list-open-jobs)
echo "$output"

if echo "$output" | grep -Eq '^[[:space:]]*[0-9]+\s+CANCELLED'; then
  echo "Job cancellation detected successfully!"
else
  echo "Unexpected job status:"
  echo "$output"
  exit 1
fi
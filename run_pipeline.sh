#!/bin/bash
# ---------------------------------------------------------------------------
# run_pipeline.sh
#
# One-command entry point for the full pipeline: submits
# submit_batch_download.sh, then submits submit_sort_zarr.sh with a
# Slurm dependency (--dependency=afterok:<download_job_id>) so it only
# starts once the download job finishes successfully.
#
# This is a plain bash script, NOT itself submitted via sbatch -- run
# it directly on the login node. It just calls `sbatch` twice and
# exits; the actual work happens in the two separately-scheduled jobs.
#
# afterok means: the sort job runs only if the download job exits with
# code 0. batch_download.py's main() exits 0 even if some individual
# dates failed internally (those are logged, not fatal to the whole
# run) -- so the sort job WILL still run in that case, on a partially-
# complete store. Check logs/pipeline.log for any FAILED entries after
# the pipeline finishes, and re-run submit_batch_download.sh to pick up
# any remaining dates if needed (resume-safe).
#
# If the download job is killed by the walltime limit instead (not a
# clean exit), it won't return 0, so the sort job correctly stays
# pending / never runs. Resubmit submit_batch_download.sh in that case.
#
# Usage:
#   ./run_pipeline.sh
# ---------------------------------------------------------------------------

set -eo pipefail

echo "Submitting batch download job..."
DOWNLOAD_JOB_OUTPUT=$(sbatch submit_batch_download.sh)
echo "$DOWNLOAD_JOB_OUTPUT"

DOWNLOAD_JOB_ID=$(echo "$DOWNLOAD_JOB_OUTPUT" | awk '{print $4}')

if [ -z "$DOWNLOAD_JOB_ID" ]; then
    echo "Could not parse job ID from sbatch output. Aborting -- sort job not submitted."
    exit 1
fi

echo "Download job ID: $DOWNLOAD_JOB_ID"
echo "Submitting sort job, dependent on download job completing successfully..."

SORT_JOB_OUTPUT=$(sbatch --dependency=afterok:"$DOWNLOAD_JOB_ID" submit_sort_zarr.sh)
echo "$SORT_JOB_OUTPUT"

SORT_JOB_ID=$(echo "$SORT_JOB_OUTPUT" | awk '{print $4}')

echo ""
echo "Both jobs submitted:"
echo "  Download job: $DOWNLOAD_JOB_ID"
echo "  Sort job:     $SORT_JOB_ID (will start after $DOWNLOAD_JOB_ID completes with exit code 0)"
echo ""
echo "Monitor with: squeue -u \$USER"
echo "Pipeline log: tail -f logs/pipeline.log"
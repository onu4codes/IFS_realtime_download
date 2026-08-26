#!/bin/bash
#SBATCH --job-name=sort_s2s_realtime_zarr
#SBATCH --output=slurm_sort_zarr_%j.log
#SBATCH --error=slurm_sort_zarr_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --partition=general
##SBATCH --account=CHANGE_ME_IF_REQUIRED
# ---------------------------------------------------------------------------
# submit_sort_zarr.sh
#
# Slurm wrapper for sort_zarr_by_time.py -- sorts the main zarr store's
# time axis ascending, writing the result to its configured sorted
# counterpart (config_paths.json -> zarr_store_sorted).
#
# Adapted from the reforecast pipeline's submit_sort_zarr.sh:
#   --paths-key is now REQUIRED (no 'default' entry in config_paths.json)
#   --time / --mem reduced (04:00:00 -> 01:00:00, 32G -> 16G) as starting
#     defaults -- a real-time store has far fewer 'time' values than the
#     reforecast store for the same date range (one per initialization
#     date instead of ~20 hindcast years per model date), so the
#     in-memory sort should be proportionally lighter. Adjust based on
#     observed store size for your chosen --paths-key.
#
# All actual progress/errors go to the single shared pipeline log file
# for this paths_key (config_paths.json -> log_file) -- the
# slurm_sort_zarr_<jobid>.log/.err files here only capture Slurm's own
# job-level stdout/stderr.
#
# Normally this is submitted automatically as part of run_pipeline.sh
# (chained after submit_batch_download.sh via a Slurm dependency), but
# it can also be run standalone any time you want to re-sort without
# rerunning the download.
#
# BEFORE SUBMITTING, edit:
#   --partition   : set to a valid partition on your cluster
#   --account     : uncomment and set if your cluster requires one
#   --paths-key in the python command below : must match whichever
#                    config_paths.json entry the batch download job used
#
# Submit with:
#   sbatch submit_sort_zarr.sh
#
# Monitor with:
#   squeue -u $USER
#   tail -f logs/pipeline_realtime_2025.log
# ---------------------------------------------------------------------------
set -eo pipefail  # deliberately no -u: conda's own activation hooks
                   # reference unset variables and break under nounset
echo "Job started on $(hostname) at $(date)"
echo "Job ID: $SLURM_JOB_ID"
source /opt/conda/etc/profile.d/conda.sh
conda activate climate
cd "$SLURM_SUBMIT_DIR"
python sort_zarr_by_time.py \
    --paths-key s2s_realtime_2025 \
    --chunk-time 20
echo "Job finished at $(date)"
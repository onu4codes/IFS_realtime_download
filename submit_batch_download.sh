#!/bin/bash
#SBATCH --job-name=s2s_realtime_batch_download
#SBATCH --output=slurm_batch_download_%j.log
#SBATCH --error=slurm_batch_download_%j.err
#SBATCH --time=04:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
##SBATCH --account=CHANGE_ME_IF_REQUIRED
# ---------------------------------------------------------------------------
# submit_batch_download.sh
#
# Slurm wrapper for batch_download.py -- downloads and converts all
# pending initialization dates (per config_model_initialization_dates.json)
# into the shared zarr store, using n_workers parallel date-workers.
#
# Adapted from the reforecast pipeline's submit_batch_download.sh:
#   --dates-config-key -> --init-dates-config-key (renamed flag)
#   --paths-key is now REQUIRED (no 'default' entry exists in this
#     repo's config_paths.json -- each initialization-dates config has
#     its own dedicated Zarr store, see config_paths.json)
#   --time reduced from 12:00:00 to 04:00:00 as a starting default --
#     real-time downloads are much faster per date than reforecast
#     (one 'time' value per date instead of ~20 hindcast years), so
#     the whole batch should complete well within the original
#     estimate; adjust based on actual observed runtime for your
#     chosen --init-dates-config-key (a full-year 'monday_thursday'
#     config has ~104 dates; a full-year 'all_dates' config has ~365).
#
# All actual progress/errors go to the single shared pipeline log file
# for this paths_key (config_paths.json -> log_file) -- the
# slurm_batch_download_<jobid>.log/.err files here only capture Slurm's
# own job-level stdout/stderr (should be nearly empty in normal runs,
# since the Python scripts print nothing to the terminal).
#
# Resume-safe: if this job is killed by the walltime limit or fails
# partway through, just resubmit -- batch_download.py's resume-skip
# logic picks up exactly where it left off (checking, per date,
# whether that date's single 'time' value is already in the store).
#
# TO RUN A DIFFERENT SCENARIO, edit the python command below:
#   --init-dates-config-key : which named date range + cadence, from
#                      config_model_initialization_dates.json (e.g.
#                      "s2s_realtime_2025" for Mon/Thu full year,
#                      "s2s_realtime_2025_daily" for daily full year,
#                      "s2s_realtime_2025_monsoon" for daily monsoon-only)
#   --paths-key      : which named output location, from config_paths.json
#                      -- each initialization-dates config in this repo
#                      has its own dedicated paths-key/Zarr store (see
#                      config_paths.json); use the matching one.
#   --variable-set   : which named group of variables to download, from
#                      config_variables.json (e.g. "combination_1" for
#                      everything, "rainfall_only" for just precip)
#   --grid-key       : which named resolution, from config_grid.json
#                      (e.g. "grids_1deg", "grids_0p25deg")
#
# BEFORE SUBMITTING, edit:
#   --partition   : set to a valid partition on your cluster
#                    (check with `sinfo`)
#   --account     : uncomment and set if your cluster requires a
#                    billing/allocation account
#                    (check with `sacctmgr show associations user=$USER`)
#   --time        : see note above -- 04:00:00 is a starting estimate,
#                    not a confirmed figure; adjust once you've observed
#                    actual per-date runtime for your scenario
#   --mem         : 32G is a generous safety margin; each worker only
#                    holds one date's data in memory at a time (a few
#                    hundred MB), so this has headroom
#   --cpus-per-task: should be >= n_workers below, plus a little
#                    overhead for the main process
#
# Submit with:
#   sbatch submit_batch_download.sh
#
# Monitor with:
#   squeue -u $USER
#   tail -f logs/pipeline_realtime_2025.log      (the actual pipeline log)
#   tail -f slurm_batch_download_<jobid>.log      (Slurm's own job log)
# ---------------------------------------------------------------------------
set -eo pipefail  # deliberately no -u: conda's own activation hooks
                   # reference unset variables and break under nounset
echo "Job started on $(hostname) at $(date)"
echo "Job ID: $SLURM_JOB_ID"
source /opt/conda/etc/profile.d/conda.sh
conda activate climate
cd "$SLURM_SUBMIT_DIR"
python batch_download.py \
    --init-dates-config-key s2s_realtime_2025_monsoon \
    --paths-key s2s_realtime_2025_monsoon_rainfall \
    --area-key south_asia_2 \
    --grid-key grids_0p25deg \
    --variable-set rainfall_only \
    --n-workers 4
echo "Job finished at $(date)"
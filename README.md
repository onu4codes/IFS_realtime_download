# IFS S2S Real-Time Forecast Download Pipeline

A config-driven pipeline for downloading ECMWF IFS Sub-seasonal to Seasonal
(S2S) **real-time forecast** data via the ECMWF Data Store (ECDS) `cdsapi`,
converting it into a merged Zarr store, and keeping it properly time-sorted --
built to run either interactively or as a Slurm batch job on a shared
cluster.

This is a sibling repository to the **IFS S2S Reforecast Download Pipeline**,
adapted for ECDS's `s2s-forecasts` dataset instead of `s2s-reforecasts`. Read
[Differences from the reforecast pipeline](#differences-from-the-reforecast-pipeline)
if you're already familiar with that repo -- most of the design and code is
directly reused.

**Status: verified working end-to-end.** Every piece of this pipeline --
variable names, request fields, area/grid subsetting, and the Zarr
append/store logic -- has been run against the live `s2s-forecasts` dataset
and produces a correctly time-indexed multi-date Zarr store. See
[Verification history](#verification-history) for exactly what was tested
and when.

---

## Table of contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Repository structure](#repository-structure)
4. [Data specification](#data-specification)
5. [Config files](#config-files)
6. [`utils/` modules](#utils-modules)
7. [Top-level scripts](#top-level-scripts)
8. [Typical workflows](#typical-workflows)
9. [Adding a new scenario](#adding-a-new-scenario)
10. [Differences from the reforecast pipeline](#differences-from-the-reforecast-pipeline)
11. [Verification history](#verification-history)
12. [Known gotchas and lessons learned](#known-gotchas-and-lessons-learned)
13. [Troubleshooting](#troubleshooting)
14. [Sharing output data](#sharing-output-data)

---

## Overview

This pipeline downloads S2S IFS **real-time forecast** data for a
configurable set of variables, area, grid resolution, and date range, and
assembles it into a single Zarr store with the structure:

```
Dimensions: (time, step, lat, lon)
  time -> forecast initialization dates (e.g. 2025-04-21, 2025-04-22, ...)
  step -> integer lead-day index, 0 to 42
  lat, lon -> the configured grid
```

Unlike the reforecast pipeline, there is **no hindcast-year dimension** --
each initialization date produces exactly one `time` value, not a block of
~20 hindcast years.

Everything that varies between runs -- which variables, which region, which
resolution, which dates, where output lands -- is controlled by JSON config
files, not by editing Python code. The only things you should ever need to
touch to run a new scenario are the config files and command-line flags.

---

## Prerequisites

### Software

```bash
pip install cdsapi xarray cfgrib zarr dask numpy
```

### ECDS credentials

Register at https://ecds.ecmwf.int/, then save your API key to `~/.cdsapirc`:

```
url: https://ecds.ecmwf.int/api
key: <your-api-key>
```

Accept the S2S dataset licence on the ECDS website before your first request,
or requests will fail with a permissions error regardless of everything else
being correct.

**Real-time access delay:** unlike the reforecast archive (accessible without
restriction), real-time S2S data is released with a delay after the
forecast's initialization time -- the delay varies by contributing centre.
Requesting a very recent initialization date before its delay window has
elapsed will fail, even if everything else about the request is correct.
Check the S2S licence page on ECDS for the current delay applicable to
`origin: ecmwf`.

### Conda environment note (cluster-specific)

If running under Slurm, `conda activate` inside a non-interactive batch shell
does **not** reliably work via `source ~/.bashrc` -- many `.bashrc` files have
an early-exit guard for non-interactive shells that skips the conda init
lines. Source conda directly instead:

```bash
source /opt/conda/etc/profile.d/conda.sh
conda activate climate
```

Also, do **not** use `set -euo pipefail` in Slurm scripts that activate
conda -- conda's own activation/deactivation hook scripts reference unset
variables and will crash under `-u` (nounset). Use `set -eo pipefail`
(no `-u`) instead.

---

## Repository structure

```
IFS_S2S_realtime_download/
├── config/
│   ├── config_area.json
│   ├── config_grid.json
│   ├── config_variables.json
│   ├── config_model_initialization_dates.json
│   └── config_paths.json
├── utils/
│   ├── __init__.py
│   ├── config_loader.py
│   ├── date_generator.py
│   ├── request_builder.py
│   ├── postprocess.py
│   └── zarr_writer.py
├── download_single_date.py
├── batch_download.py
├── sort_zarr_by_time.py
├── submit_batch_download.sh
├── submit_sort_zarr.sh
├── run_pipeline.sh
├── .gitignore
└── README.md
```

At runtime, additional paths get created as siblings to `config/` and
`utils/` (all gitignored) -- **one full set per `paths_key`**, since each
named initialization-dates config in this repo has its own dedicated output
location (see [Config files](#config-files)):

```
├── s2s_realtime_2025_monsoon.zarr/            (main output for this paths_key)
├── s2s_realtime_2025_monsoon_sorted.zarr/     (time-sorted counterpart)
├── s2s_work_realtime_2025_monsoon/            (scratch dir for in-flight GRIB downloads)
└── logs/
    └── pipeline_realtime_2025_monsoon.log     (shared log file for this paths_key)
```

---

## Data specification

The default configuration (`combination_1` variable set, `south_asia` area,
`grids_1deg` grid) produces:

| Property | Value |
|---|---|
| Area | 38.5N-6.5N, 66.5E-100.5E (South Asia) |
| Grid | 1.0 x 1.0 degree |
| Lead time | 0-42 days (real-time supports up to 46 days / 1104h natively -- capped at 42 to match the reforecast pipeline; see [Verification history](#verification-history)) |
| Ensemble | control forecast only |
| Cadence | see [`config_model_initialization_dates.json`](#config_model_initialization_datesjson) -- varies by date range |
| Origin | ecmwf (IFS) |
| Dataset | `s2s-forecasts` (ECDS real-time archive) |

### Variable groups (in `combination_1`)

Identical to the reforecast pipeline -- reused verbatim, and **confirmed
working against a live `s2s-forecasts` request** (see
[Verification history](#verification-history)).

| Group | Variables | Level type | Postprocessing |
|---|---|---|---|
| `wind_msl` | 10m U/V wind, MSLP | single_level | none |
| `precip` | total precipitation | single_level | deaccumulate (cumulative -> true daily totals) |
| `t2m` | 2m temperature | single_level | realign period-mean steps onto common axis |
| `pressure_level_vars` | specific humidity, temperature, U/V wind | pressure (1000/850/500/200 hPa) | flatten pressure levels into separate named variables |

Final output variables (21 total, `combination_1`): `10m_u_component_of_wind`,
`10m_v_component_of_wind`, `mean_sea_level_pressure`, `total_precipitation`,
`2m_temperature`, and 4 pressure levels each of `specific_humidity_*`,
`temperature_*`, `u_component_of_wind_*`, `v_component_of_wind_*`.

`combination_2` (15 variables) additionally includes geopotential height and
soil moisture / total column water -- see `config_variables.json` for the
exact group definitions.

**Important quirk (unchanged from reforecast):** `2m_temperature` has `NaN`
at `step=0`. This is correct, not a bug -- `t2m` is requested as
period-mean ranges (`"0_24"`, `"24_48"`, ...), and there is no valid "mean
over day 0" (day 0 is the instantaneous initialization point, not a 24-hour
window).

---

## Config files

All config files live in `config/` and use a consistent **named-entry**
pattern: each file is a JSON object whose top-level keys are names you choose,
and whose values are the actual settings. This lets you add new scenarios by
adding a new named entry, without ever touching Python code.

### `config_area.json` / `config_grid.json`

Reused unchanged from the reforecast pipeline. Same structure, same named
entries. Area/grid subsetting is confirmed working on `s2s-forecasts` (see
[Verification history](#verification-history)).

### `config_variables.json`

Reused **verbatim** from the reforecast pipeline (`combination_1`,
`combination_2`, `rainfall_only`). **Confirmed correct** against a live
`s2s-forecasts` request -- the `cds_variable` naming (including the
underscore-placement convention, e.g. `10_m_u_component_of_wind`) is
identical between `s2s-forecasts` and `s2s-reforecasts`.

### `config_model_initialization_dates.json`

Renamed from the reforecast pipeline's `config_model_version_dates.json` --
real-time forecasts have an *initialization date* (the day the forecast was
run), not a *model version date* tied to a hindcast archive.

```json
{
  "s2s_realtime_2025": {
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "cadence": "monday_thursday",
    "description": "IFS S2S real-time forecast cadence, full year 2025"
  },
  "s2s_realtime_2025_daily": {
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "cadence": "all_dates",
    "description": "IFS S2S real-time forecast, full year 2025, all calendar days (model runs daily)"
  },
  "s2s_realtime_2025_monsoon": {
    "start_date": "2025-04-21",
    "end_date": "2025-10-04",
    "cadence": "all_dates",
    "description": "IFS S2S real-time forecast, monsoon season only (April 21 to October 4, 2025), all calendar days (model runs daily)"
  }
}
```

Same shape as the reforecast pipeline's dates config, **minus**
`hindcast_years_back` -- real-time forecasts have no hindcast expansion, so
that key doesn't exist here at all (not optional -- genuinely absent, and
not read by any code in this repo).

`cadence` is a named rule recognized by `utils/date_generator.py`. Two are
currently implemented:

| Cadence | Meaning |
|---|---|
| `monday_thursday` | Only Mondays and Thursdays in the date range |
| `all_dates` | Every calendar day in the date range |

**ECMWF's real-time S2S cadence has changed over time** -- earlier periods
ran Monday/Thursday only; later periods run daily. Use `monday_thursday` for
periods before that switch and `all_dates` for periods after. **Confirm the
exact cutover date against ECMWF's own S2S real-time schedule documentation**
before adding a new date-range entry that might straddle it -- getting this
wrong for `monday_thursday` periods will silently attempt (and fail) most
daily requests, and getting it wrong for `all_dates` periods on an
already-daily period is harmless but wastes requests on non-existent init
dates. The 2025 monsoon window above is treated as daily, per direct
confirmation that the model was already running daily by then.

Add a new cadence by adding both a new entry here and a new handler function
in `date_generator.py`: write a function with signature
`(start_date, end_date) -> list[date]`, then register it in
`_CADENCE_HANDLERS`. No other code needs to change -- see
`date_generator.py`'s module docstring.

### `config_paths.json`

**One dedicated `paths_key` per initialization-dates config entry** -- unlike
the reforecast pipeline (which partitions `paths_key` only by
area/grid/variable-set compatibility, and lets multiple date ranges share a
store), this repo gives every named date-range/cadence combination its own
Zarr store, even when they'd otherwise be dimensionally compatible. This was
a deliberate choice to avoid ambiguity between overlapping date-range configs
(e.g. `s2s_realtime_2025` and `s2s_realtime_2025_daily` both cover the same
calendar year via different cadence rules) -- keeping them fully separate
means there's no risk of one run's resume-check logic getting confused by
another run's data in the same store.

**Trade-off:** if you want one continuous real-time archive spanning
multiple date-range configs (e.g. combining a Mon/Thu period with a
following daily period into a single time series), you'll need to merge the
resulting Zarr stores yourself downstream -- this repo does not do that
automatically.

```json
{
  "s2s_realtime_2025": {
    "zarr_store": "s2s_realtime_2025.zarr",
    "zarr_store_sorted": "s2s_realtime_2025_sorted.zarr",
    "workdir": "s2s_work_realtime_2025",
    "log_file": "logs/pipeline_realtime_2025.log",
    "description": "Real-time forecast, full year 2025, Monday/Thursday cadence"
  },
  "s2s_realtime_2025_daily": {
    "zarr_store": "s2s_realtime_2025_daily.zarr",
    "zarr_store_sorted": "s2s_realtime_2025_daily_sorted.zarr",
    "workdir": "s2s_work_realtime_2025_daily",
    "log_file": "logs/pipeline_realtime_2025_daily.log",
    "description": "Real-time forecast, full year 2025, daily cadence"
  },
  "s2s_realtime_2025_monsoon": {
    "zarr_store": "s2s_realtime_2025_monsoon.zarr",
    "zarr_store_sorted": "s2s_realtime_2025_monsoon_sorted.zarr",
    "workdir": "s2s_work_realtime_2025_monsoon",
    "log_file": "logs/pipeline_realtime_2025_monsoon.log",
    "description": "Real-time forecast, monsoon season 2025 (April 21 to October 4), daily cadence"
  }
}
```

There is **no `default` entry** -- `--paths-key` is a required argument on
every top-level script in this repo, not an optional one with a fallback.
All paths are relative to the **repo root** (parent of `config/` and
`utils/`), resolved to absolute paths regardless of the caller's current
working directory.

---

## `utils/` modules

| Module | Status | Purpose |
|---|---|---|
| `config_loader.py` | Adapted | Loads and validates all 5 config files; provides `get_area()`, `get_grid()`, `get_variable_group()`, `get_model_init_dates_config()`, `get_zarr_path()`, etc. `paths_key` arguments are required (no default) throughout. |
| `date_generator.py` | Adapted | `generate_initialization_dates(config_key)` applies the named cadence rule and returns the full list of initialization dates. No hindcast-year pairing -- there's nothing analogous to the reforecast pipeline's `get_hindcast_years()`. |
| `request_builder.py` | Adapted | `build_request(group_name, model_date, area_key, grid_key, variable_set=...)` assembles the `cdsapi` request dict against the `s2s-forecasts` dataset. No `hyear_list` parameter, no `hyear`/`hmonth`/`hday` fields in the request. Field structure confirmed correct against a working sample request (see [Verification history](#verification-history)). |
| `postprocess.py` | **Unchanged** | `deaccumulate()`, `realign_step_range_end_labeled()`, `flatten_pressure_levels()`, dispatched via `apply_postprocess(ds, group_config)`. Operates only on `step`, entirely agnostic to hindcast vs. real-time. |
| `zarr_writer.py` | **Adapted (bug fix)** | `process_group_file()`, `build_dataset()`, `write_zarr()`, `group_filename()`. **`build_dataset()` explicitly promotes `time` to a length-1 dimension via `expand_dims("time")` before returning** -- this is a real-time-specific fix, not present in the reforecast pipeline. See [Known gotchas](#known-gotchas-and-lessons-learned) for why this was necessary. |

---

## Top-level scripts

### `download_single_date.py`

Downloads all (or a subset of) variable groups for **one** initialization
date. Exposes `download_single_date_files(...)` as an importable function
(used by `batch_download.py`) plus a CLI.

Unlike the reforecast pipeline, this script has **no `--dates-config-key`
argument at all** -- that parameter only ever existed to look up
`hindcast_years_back`, which doesn't exist here. `--paths-key` is now
required (no `default`).

```bash
python download_single_date.py --model-date 2025-06-02 --paths-key s2s_realtime_2025
python download_single_date.py --model-date 2025-06-02 --paths-key s2s_realtime_2025_daily --variable-set combination_1
python download_single_date.py --model-date 2025-06-02 --paths-key s2s_realtime_2025 --groups wind_msl precip
```

Fail-fast: if any group's download fails, the exception propagates
immediately -- it does not attempt remaining groups, and does not clean up
partially-downloaded files (that decision belongs to the caller).

### `batch_download.py`

Orchestrates the full pipeline across all initialization dates in a named
`config_model_initialization_dates.json` entry, with 4 (configurable) dates
processed in parallel. Each worker downloads + builds the merged dataset;
only the **main process** ever writes to the Zarr store (serially, to avoid
concurrent-write corruption).

```bash
python batch_download.py --init-dates-config-key s2s_realtime_2025 --paths-key s2s_realtime_2025
python batch_download.py --init-dates-config-key s2s_realtime_2025_daily --paths-key s2s_realtime_2025_daily --n-workers 4
python batch_download.py --init-dates-config-key s2s_realtime_2025_monsoon --paths-key s2s_realtime_2025_monsoon
```

**Resume-safe:** at startup, reads the target Zarr store's existing `time`
values once; any initialization date whose own `time` value is already
present is skipped. Simpler than the reforecast pipeline's resume-check
(which had to verify a whole set of hindcast years per model date) --
here it's a single membership check per date. Safe to re-run the same
command after any interruption or partial failure.

**Cleanup policy:** GRIB files are deleted after a date's data is
successfully appended to the Zarr store; kept (for debugging) if that date
failed.

### `sort_zarr_by_time.py`

Sorts the Zarr store's `time` axis, writing the result to the configured
`zarr_store_sorted` path (always overwritten fresh). Logic is completely
unchanged from the reforecast pipeline -- see the module docstring for why
out-of-order values can still occur even with one `time` value per append
(parallel workers finishing out of submission order).

```bash
python sort_zarr_by_time.py --paths-key s2s_realtime_2025_monsoon
```

### Slurm scripts

| Script | Purpose |
|---|---|
| `submit_batch_download.sh` | Slurm wrapper for `batch_download.py`. Edit `--partition`, `--account` (if required), and the `python batch_download.py` flags for your scenario before submitting. |
| `submit_sort_zarr.sh` | Slurm wrapper for `sort_zarr_by_time.py`. |
| `run_pipeline.sh` | Plain bash (not itself an `sbatch` job) that submits both jobs above, chaining the sort job with `--dependency=afterok:<download_job_id>`. Reused unchanged from the reforecast pipeline. |

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

---

## Typical workflows

### First-time setup: verify one date before a full batch run

```bash
python download_single_date.py --model-date 2025-04-22 --paths-key s2s_realtime_2025_monsoon
```

Pick a `--model-date` safely past the real-time release delay. Inspect the
downloaded GRIB structure to confirm variable names and area/grid subsetting
resolved correctly:

```bash
python3 -c "
import xarray as xr
ds = xr.open_dataset('s2s_work_realtime_2025_monsoon/s2s_wind_msl_20250422.grib', engine='cfgrib', backend_kwargs={'indexpath': ''})
print(ds.latitude.min().item(), ds.latitude.max().item())
print(ds.longitude.min().item(), ds.longitude.max().item())
"
```

### Full batch run, locally (no Slurm)

```bash
python batch_download.py --init-dates-config-key s2s_realtime_2025_monsoon --paths-key s2s_realtime_2025_monsoon --n-workers 4
```

### Full batch run, via Slurm

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
squeue -u $USER
tail -f logs/pipeline_realtime_2025_monsoon.log
```

### Checking progress mid-run

```bash
grep -c "SUCCESS" logs/pipeline_realtime_2025_monsoon.log
grep -c "FAILED" logs/pipeline_realtime_2025_monsoon.log
python3 -c "
import xarray as xr
ds = xr.open_zarr('s2s_realtime_2025_monsoon.zarr')
print(ds.sizes)
print(ds['time'].values)
"
```

`ds.sizes` should show `time` growing toward the expected date count as the
batch progresses -- if `time` is missing from `ds.sizes` entirely, or stuck
at 1 no matter how many dates have completed, see
[Known gotchas](#known-gotchas-and-lessons-learned) (the `expand_dims` fix
must be present in your `utils/zarr_writer.py`).

### Resuming after an interruption or partial failure

Just re-run the same command -- resume-skip logic handles the rest.

---

## Adding a new scenario

Same table as the reforecast pipeline, with one addition (initialization
dates now also require a matching new `paths_key`, since this repo doesn't
share stores across date-range configs):

| Want to change... | Edit this file | Then pass |
|---|---|---|
| Region | `config_area.json`: add a new named entry | `--area-key <name>` |
| Resolution | `config_grid.json`: add a new named entry | `--grid-key <name>` |
| Which variables | `config_variables.json`: add a new named **variable set** | `--variable-set <name>` |
| Date range / cadence | `config_model_initialization_dates.json`: add a new named entry (choose `monday_thursday` or `all_dates` based on ECMWF's schedule for that period) | `--init-dates-config-key <name>` |
| Output location | `config_paths.json`: add a new named entry (**always required** for a new date-range config in this repo, even if area/grid/variable-set match an existing run) | `--paths-key <name>` |

---

## Differences from the reforecast pipeline

For anyone familiar with the reforecast repo, this section summarizes every
change made to produce this real-time repo.

**Fully unchanged (byte-for-byte reused logic):**
- `utils/postprocess.py`
- `sort_zarr_by_time.py` (logic unchanged; docstring updated to explain the
  real-time-specific reason sorting can still matter)
- `run_pipeline.sh`

**Reused structure, new content only:**
- `config_area.json`, `config_grid.json` -- same named entries
- `config_variables.json` -- reused verbatim, **confirmed correct** against
  live `s2s-forecasts` requests

**Renamed and adapted:**
- `config_model_version_dates.json` -> `config_model_initialization_dates.json`
  -- `hindcast_years_back` dropped from required/recognized keys; two new
  cadence rules (`monday_thursday`, `all_dates`) replace `odd_day_of_month`
- `config_loader.py` -- `MODEL_DATES_CONFIG_FILE` ->
  `MODEL_INIT_DATES_CONFIG_FILE`, `load_model_dates_config()` ->
  `load_model_init_dates_config()`, `get_model_dates_config()` ->
  `get_model_init_dates_config()`; `paths_key` parameters no longer default
  to `"default"` (no such entry exists in this repo's `config_paths.json`)
- `date_generator.py` -- `generate_model_dates()` ->
  `generate_initialization_dates()`; `get_hindcast_years()` and
  `get_model_dates_with_hindcast_years()` removed entirely (no hindcast
  concept); two cadence handlers instead of one
- `request_builder.py` -- `DATASET` changed to `"s2s-forecasts"`;
  `hyear`/`hmonth`/`hday` fields removed from the request dict; no
  `hyear_list` parameter on `build_request()`
- `download_single_date.py` -- `--dates-config-key` removed entirely (it
  only ever existed to look up `hindcast_years_back`); `--paths-key` is now
  required
- `batch_download.py` -- `--dates-config-key` renamed to
  `--init-dates-config-key`; `--paths-key` is now required; resume-check
  simplified from "is this date's full hindcast-year set present" to "is
  this date's single `time` value present"
- `submit_batch_download.sh` / `submit_sort_zarr.sh` -- flag names/values
  updated to match; `--time`/`--mem` defaults reduced as starting estimates
  (real-time batches are lighter per-date than reforecast batches)

**Genuine bug fix, not present in the reforecast pipeline's design:**
- `zarr_writer.py`'s `build_dataset()` -- added an explicit
  `ds.expand_dims("time")` step. This was NOT part of the original adaptation
  plan; it was discovered only after running the real pipeline against real
  data (see [Verification history](#verification-history) and
  [Known gotchas](#known-gotchas-and-lessons-learned) for the full story).

**New design decision unique to this repo:**
- `config_paths.json` gives every initialization-dates config entry its own
  dedicated Zarr store, rather than sharing stores across date ranges the
  way the reforecast pipeline does.

---

## Verification history

This pipeline was built by adapting the reforecast pipeline's code and
config, then validated in stages against the real `s2s-forecasts` dataset.
Recorded here so the level of confidence behind each part of the pipeline is
traceable, not just asserted.

1. **Variable naming.** A working, independently-run `cdsapi` script against
   `s2s-forecasts` (dataset, `origin: "ecmwf"`, `forecast_type:
   "control_forecast"`) confirmed `10_m_u_component_of_wind`,
   `10_m_v_component_of_wind`, `total_precipitation`, and `2_m_temperature`
   resolve correctly -- identical strings to `config_variables.json`,
   underscore placement included. This resolved the one open item flagged
   when `config_variables.json` was first reused from the reforecast repo.

2. **Request field structure.** The same working script confirmed no
   `hyear`/`hmonth`/`hday` fields are needed or expected, and that
   `year`/`month`/`day`/`time`/`level_type`/`variable`/`forecast_type`/
   `leadtime_hour`/`data_format` match `request_builder.py`'s output field
   for field. The reference script didn't include `area`/`grid` (it pulled
   global, native-resolution data), so those two fields weren't verified by
   that script directly.

3. **Real-time lead time availability.** The reference script's
   `leadtime_hour` list extended to `1104` (46 days) in both point (6-hourly)
   and range (24-hourly) formats -- confirming real-time supports a longer
   forecast horizon than the 42-day reforecast archive, consistent with
   ECMWF's documented real-time schedule. This repo intentionally caps at
   1008h/42 days to match the reforecast pipeline, per an explicit decision
   made when the repo was scoped -- there's confirmed headroom to extend
   later if needed.

4. **`time`-dimension bug, found and fixed via a real production run.** The
   first full pipeline run (via `download_single_date.py` +
   `batch_download.py`, `s2s_realtime_2025_monsoon`) produced a Zarr store
   whose `time` coordinate was a 0-dimensional scalar, not a real dimension
   -- `xr.open_zarr(...).sizes` showed no `time` entry at all. Root cause:
   the reforecast pipeline's `zarr_writer.py` was reused unchanged on the
   assumption that cfgrib always decodes `time` as a dimension -- true when
   multiple hindcast years are requested (reforecast), false when only one
   initialization date is requested (real-time; cfgrib decodes a single
   value as a scalar coordinate instead).

   Testing confirmed this wasn't just a missing feature but **active silent
   data loss**: appending a second date via `to_zarr(mode="a",
   append_dim="time")` against a store with scalar `time` did not raise an
   error -- it silently **overwrote the entire store** with only the newest
   date, discarding everything previously written, with no warning of any
   kind.

   Fix: `build_dataset()` in `zarr_writer.py` now calls
   `merged.expand_dims("time")` whenever `"time" not in merged.dims`,
   before returning. Re-verified afterward with a real two-date run
   (`2025-04-21`, `2025-04-22`) against `s2s_realtime_2025_monsoon.zarr`:
   the resulting store correctly shows `time: 2`, both dates present and
   ascending, all 15 `combination_2` variables carrying the full
   `(time, step, lat, lon)` dim signature, and `lat`/`lon` bounds matching
   `south_asia`'s configured area -- which also serves as the confirmation
   that `area`/`grid` subsetting (point 2 above, left unverified by the
   reference script) works correctly on `s2s-forecasts`.

   **If you're running an older copy of this repo predating this fix**,
   delete any existing Zarr store built with it before re-running -- its
   contents cannot be trusted (see
   [Known gotchas](#known-gotchas-and-lessons-learned) below for the
   recovery steps).

---

## Known gotchas and lessons learned

Carried over from the reforecast pipeline (still applicable):

- **Config is read fresh on every single call, with no caching.** Never
  edit config files while a batch job is actively running.
- **JSON has no comment syntax.** Use separate named variable sets instead
  of commenting out a group.
- **CDS variable naming is picky about underscore placement** (e.g.
  `10_m_u_component_of_wind`, not `10m_u_component_of_wind`). A malformed
  name doesn't error clearly -- MARS may silently try to interpret it as
  something else entirely.
- **`grid` overrides work, but must be explicit** -- without one, ECDS
  returns data at native resolution, not a coarse default.
- **`2m_temperature`'s period-mean range format labels each step by its
  END, not its start** -- offset by one relative to point-type groups, and
  has no `step=0`.
- **Total precipitation is a running cumulative total, not per-day
  totals** -- `postprocess.deaccumulate()` handles this.
- **Rewriting a Zarr store with different chunking requires clearing stale
  chunk encoding first.**
- **`xr.open_zarr()`'s consolidated-metadata cache can appear stale
  immediately after an append.**
- **Conda activation inside Slurm needs
  `source /opt/conda/etc/profile.d/conda.sh`**, and avoid `set -u`.

New for this repo:

- **Real-time access delay.** A request for a very recent initialization
  date may fail if ECMWF's release delay for that date hasn't elapsed yet
  -- this is not a bug in the pipeline, retry later.
- **Cadence must match the actual ECMWF schedule for that period.**
  `monday_thursday` vs. `all_dates` is not a free choice -- using the wrong
  one either wastes requests on non-existent initialization dates
  (`all_dates` applied to a Mon/Thu-only period) or silently skips real
  dates the model actually produced (`monday_thursday` applied to an
  already-daily period). Confirm against ECMWF's schedule documentation
  before adding a new date-range entry.
- **`time` must be a real dimension, not a scalar coordinate, before any
  Zarr write.** This is the single most important gotcha in this repo --
  see [Verification history](#verification-history) point 4 for the full
  story. `zarr_writer.py`'s `build_dataset()` handles this automatically as
  of the current version (`expand_dims("time")`), but if you ever write a
  custom script that builds a dataset from a single-date GRIB file and
  writes it to Zarr **without** going through `zarr_writer.build_dataset()`,
  you must apply this fix yourself, or later appends will silently destroy
  earlier data with no error raised.

  **Recovery if you have a store built before this fix:** the store's
  contents are not trustworthy (silent overwrite means it likely contains
  only your most recently-run date, not the full expected set). Delete it
  and the download workdir, confirm your `utils/zarr_writer.py` includes
  the `expand_dims` fix, and re-run the batch download from scratch --
  resume-skip logic will treat the empty store as nothing-done and process
  every date fresh:
  ```bash
  rm -rf <zarr_store> <zarr_store_sorted> <workdir>
  python batch_download.py --init-dates-config-key <key> --paths-key <key>
  ```

- **Real-time supports up to 46 days (1104h) of lead time and 6-hourly
  point-type steps natively** -- this repo intentionally requests only
  0-42 days at daily resolution to match the reforecast pipeline. If you
  ever want finer/longer real-time data, `leadtime_end`/`leadtime_step` in
  `config_variables.json` can be adjusted per group; confirmed headroom
  exists on the ECDS side (see
  [Verification history](#verification-history) point 3).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `mars - ERROR - Ambiguous: X could be Y or Z` | Malformed CDS variable name (wrong underscore placement) | Check `cds_variable` entries in `config_variables.json` |
| Permissions/access error for a very recent `--model-date` | Real-time release delay hasn't elapsed yet for that date | Wait and retry later; check the S2S licence page on ECDS for the current delay |
| `400 Client Error: Bad Request`, isolated to one date sandwiched between successes | Transient ECDS server-side issue | Retry -- re-run the same batch command, resume-skip will only retry the failed date |
| `CondaError: Run 'conda init' before 'conda activate'` in a Slurm job | Non-interactive shell doesn't source `~/.bashrc`'s conda init lines | Use `source /opt/conda/etc/profile.d/conda.sh` directly |
| Large fraction of dates fail, all with the same generic error, right after a specific timestamp | Config file was edited while the batch job was running | Fix the config, then re-run -- resume-skip handles the rest |
| Most dates in a `monday_thursday`-cadence run fail with "no data" style errors | Wrong cadence for this period -- the model may already be running daily | Switch that date range's `cadence` to `all_dates` in `config_model_initialization_dates.json` |
| `ds.sizes` has no `time` entry at all, or `time` shows as a 0-d scalar when you inspect the store | Store was built with a `zarr_writer.py` missing the `expand_dims("time")` fix -- **store contents are not trustworthy, likely only the latest date survived** | Delete the store and workdir, confirm the fix is present, re-run the full batch from scratch (see [Known gotchas](#known-gotchas-and-lessons-learned)) |
| Batch run appears to succeed (no errors logged) but `ds.sizes['time']` never grows past 1 no matter how many dates complete | Same root cause as above -- each "append" is silently overwriting the store instead of adding to it | Same fix as above |
| `ds.sizes['time']` looks smaller than expected right after an append, but you've confirmed the `expand_dims` fix is present | Stale consolidated-metadata cache (not the scalar-time bug) | `xr.open_zarr(path, consolidated=False)` or `zarr.consolidate_metadata(path)` |
| Zarr sort fails with a chunk-encoding conflict | Rechunking without clearing inherited chunk encoding | See `sort_zarr_by_time.py`'s encoding-clearing step |

---

## Sharing output data

Same as the reforecast pipeline -- a Zarr store is a directory, not a single
file. Package it into a single archive before sharing:

```bash
tar -cvf s2s_realtime_2025_monsoon_sorted.tar s2s_realtime_2025_monsoon_sorted.zarr
```

Recipients extract with:

```bash
tar -xvf s2s_realtime_2025_monsoon_sorted.tar
```

For large transfers over an unstable connection, prefer `rsync -avP` over
`scp` -- it supports resuming an interrupted transfer.
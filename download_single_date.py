#!/usr/bin/env python3
"""
download_single_date.py

Top-level entry point: download all variable groups (from
config_variables.json) for ONE forecast initialization date, using the
config-driven pipeline (config_loader, request_builder).

Exposes download_single_date_files() as an importable function so
batch_download.py can call it directly (no subprocess spawning), plus
a CLI wrapper for standalone runs.

Adapted from the reforecast pipeline's download_single_date.py for the
real-time pipeline: no hindcast-year expansion, so this script no
longer needs a --dates-config-key argument at all -- that parameter
only ever existed here to look up 'hindcast_years_back' for building
hyear_list, and real-time has no such concept. The initialization
dates config (config_model_initialization_dates.json) is only consulted
by batch_download.py, to generate the list of dates to loop over; this
script just downloads whatever single --model-date it's given.

Fail-fast: if any group's download fails, the exception propagates
immediately -- it does not attempt remaining groups, and does not clean
up partially-downloaded files (that decision belongs to the caller).

Per project convention: nothing is printed to the terminal. All
progress/errors go to a log file only.

CLI usage:
    python download_single_date.py --model-date 2025-06-02 --paths-key s2s_realtime_2025
    python download_single_date.py --model-date 2025-06-02 --paths-key s2s_realtime_2025 --groups wind_msl precip
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

try:
    import cdsapi
except ImportError:
    sys.exit("cdsapi is not installed. Install it with:\n    pip install cdsapi")

from utils import config_loader
from utils import request_builder
from utils import zarr_writer
from utils.config_loader import ConfigError

logger = logging.getLogger(__name__)


def download_single_date_files(
    model_date,
    paths_key,
    groups=None,
    area_key="south_asia",
    grid_key="grids_1deg",
    variable_set="combination_1",
    client=None,
):
    """
    Download all (or a specified subset of) variable groups, within a
    named variable set, for one forecast initialization date.

    Defaults to variable_set='combination_1' so existing calls that
    don't pass this parameter keep working unchanged.

    paths_key has no default (unlike the reforecast pipeline's
    'default') -- config_paths.json in this repo has one entry per
    initialization-dates config, with no single generic default, so
    the caller must always say which target store this belongs to.

    Returns a dict of {group_name: downloaded_file_path}.

    Raises on the first failure -- does not attempt remaining groups
    once one fails, and does not clean up partially-downloaded files
    (that's the caller's decision).
    """
    if groups is None:
        groups = config_loader.list_variable_groups(variable_set=variable_set)

    workdir = config_loader.get_workdir_path(paths_key)
    workdir.mkdir(parents=True, exist_ok=True)

    if client is None:
        client = cdsapi.Client()

    downloaded = {}
    for group_name in groups:
        dataset, request = request_builder.build_request(
            group_name, model_date, area_key, grid_key, variable_set=variable_set
        )
        target = workdir / zarr_writer.group_filename(group_name, model_date)

        logger.info(f"Retrieving group='{group_name}' (set='{variable_set}') model_date={model_date} -> {target}")
        client.retrieve(dataset, request).download(str(target))
        logger.info(f"Downloaded group='{group_name}' -> {target}")

        downloaded[group_name] = target

    logger.info(
        f"Completed all {len(downloaded)} group(s) for model_date={model_date} (set='{variable_set}'): "
        f"{list(downloaded.keys())}"
    )
    return downloaded


def _setup_logging(paths_key):
    """
    Configure root logger with a single FileHandler pointing at the
    shared pipeline log file for this paths_key (config_paths.json ->
    log_file). Appends rather than overwrites, so multiple runs/dates
    accumulate into one continuous record rather than one file per
    date or per invocation.

    No StreamHandler is attached -- nothing prints to the terminal.
    """
    log_path = config_loader.get_log_path(paths_key)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Avoid attaching duplicate handlers if this is somehow called more
    # than once in the same process.
    already_configured = any(
        isinstance(h, logging.FileHandler) and Path(h.baseFilename) == log_path
        for h in root_logger.handlers
    )
    if not already_configured:
        fh = logging.FileHandler(log_path, mode="a")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root_logger.addHandler(fh)

    return log_path


def _parse_args():
    p = argparse.ArgumentParser(description="Download one initialization date's S2S real-time forecast data")
    p.add_argument("--model-date", required=True, help="Forecast initialization date, YYYY-MM-DD")
    p.add_argument("--paths-key", required=True,
                    help="Named paths config key in config_paths.json (e.g. s2s_realtime_2025)")
    p.add_argument("--groups", nargs="+", default=None,
                    help="Variable group names to download (default: all groups in the chosen variable set)")
    p.add_argument("--variable-set", default="combination_1",
                    help="Named variable set key in config_variables.json (default: combination_1)")
    p.add_argument("--area-key", default="south_asia", help="Named area config key (default: south_asia)")
    p.add_argument("--grid-key", default="grids_1deg", help="Named grid config key (default: grids_1deg)")
    return p.parse_args()


def main():
    args = _parse_args()
    model_date = datetime.strptime(args.model_date, "%Y-%m-%d").date()

    log_path = _setup_logging(args.paths_key)

    try:
        download_single_date_files(
            model_date,
            args.paths_key,
            groups=args.groups,
            area_key=args.area_key,
            grid_key=args.grid_key,
            variable_set=args.variable_set,
        )
    except (ConfigError, Exception) as e:
        logger.error(f"download_single_date_files failed for {model_date}: {e}")
        raise
    finally:
        logger.info(f"Log written to {log_path}")


if __name__ == "__main__":
    main()
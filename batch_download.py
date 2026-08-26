#!/usr/bin/env python3
"""
batch_download.py

Orchestrates the full pipeline across all initialization dates in a
named config_model_initialization_dates.json entry:

    for each initialization date (up to n_workers in parallel):
        download 4 GRIB files (download_single_date.download_single_date_files)
        build the merged in-memory dataset (zarr_writer.build_dataset)
    <-- workers stop here, return the dataset to the main process -->
    main process (serial, one at a time):
        append the dataset to the shared zarr store (zarr_writer.write_zarr)
        delete that date's GRIB files on success (kept on failure, for debugging)

Workers do NOT write to the zarr store themselves -- concurrent writes
to the same store from separate processes risk corruption. Only the
main process ever calls write_zarr(), serially, one date at a time.

Adapted from the reforecast pipeline's batch_download.py for the
real-time pipeline: the resume-skip check is simpler here than in the
reforecast version. There, a model date could only be skipped once its
FULL hindcast-year set was present in the store. Here, each
initialization date corresponds to exactly one 'time' value -- so the
check is just "is this date's own time value already in the store,
yes or no". date_generator.generate_initialization_dates() replaces
get_model_dates_with_hindcast_years() as the date-list source, since
there's no hyear_list to pair each date with.

Logging: all worker processes send log records through a
multiprocessing.Queue to a single QueueListener running in the main
process, which writes them to the one shared pipeline log file for
this paths_key (config_paths.json -> log_file). This avoids the
file-corruption risk of multiple separate OS processes writing to the
same file directly. Per project convention, nothing is printed to the
terminal.

CLI usage:
    python batch_download.py --init-dates-config-key s2s_realtime_2025 --paths-key s2s_realtime_2025
    python batch_download.py --init-dates-config-key s2s_realtime_2025_monsoon --paths-key s2s_realtime_2025_monsoon --n-workers 4
"""

import argparse
import logging
import logging.handlers
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np

from utils import config_loader
from utils import date_generator
from utils import zarr_writer
import download_single_date

logger = logging.getLogger(__name__)


def _worker_logging_init(log_queue):
    """
    Run once per worker process at startup (ProcessPoolExecutor
    initializer). Configures the worker's root logger to send
    everything through the shared queue instead of writing to any
    file directly -- the main process's QueueListener does the actual
    file writing.
    """
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(logging.handlers.QueueHandler(log_queue))


def _process_one_date_worker(model_date, area_key, grid_key, paths_key, variable_set):
    """
    Runs in a worker process. Downloads all variable groups (within
    the given variable set) for one initialization date and builds the
    merged in-memory dataset. Does NOT write to the zarr store --
    returns the dataset (and downloaded file paths) to the main
    process for that.

    Returns one of:
        ("success", model_date, dataset, downloaded_paths_dict)
        ("failed", model_date, error_message)
    """
    log = logging.getLogger(__name__)
    try:
        downloaded = download_single_date.download_single_date_files(
            model_date,
            paths_key,
            area_key=area_key,
            grid_key=grid_key,
            variable_set=variable_set,
        )
        ds = zarr_writer.build_dataset(downloaded, model_date, variable_set=variable_set)
        # Force all data into memory (plain numpy, not lazy/dask-backed)
        # so the dataset can be safely pickled back to the main process.
        ds = ds.load()

        log.info(f"Worker finished download+build for model_date={model_date} (set='{variable_set}')")
        return ("success", model_date, ds, downloaded)

    except Exception as e:
        log.error(f"Worker failed for model_date={model_date} (set='{variable_set}'): {e}")
        return ("failed", model_date, str(e))


def _get_existing_zarr_times(zarr_path):
    """
    Return the set of existing 'time' values (as datetime64[D]) in the
    target zarr store, or an empty set if it doesn't exist yet.
    """
    if not Path(zarr_path).exists():
        return set()
    try:
        import xarray as xr
        ds = xr.open_zarr(zarr_path)
        return set(np.array(ds["time"].values, dtype="datetime64[D]"))
    except Exception as e:
        logger.warning(f"Could not read existing zarr times for resume check: {e}")
        return set()


def _is_date_already_done(model_date, existing_times):
    """
    Check if this initialization date's own 'time' value is already in
    the zarr store. Unlike the reforecast pipeline (which had to check
    a whole set of hindcast years per model date), real-time has
    exactly one 'time' value per initialization date, so this is a
    single membership check.
    """
    expected = np.datetime64(f"{model_date:%Y-%m-%d}", "D")
    return expected in existing_times


def run_batch_download(
    init_dates_config_key,
    paths_key,
    area_key="south_asia",
    grid_key="grids_1deg",
    variable_set="combination_1",
    n_workers=4,
):
    """
    Main orchestration function. See module docstring for the full
    pipeline description.

    Defaults to variable_set='combination_1' so existing calls that
    don't pass this parameter keep working unchanged. init_dates_config_key
    and paths_key have no defaults -- config_paths.json in this repo
    has one entry per initialization-dates config, with no single
    generic default to fall back on.
    """
    zarr_path = config_loader.get_zarr_path(paths_key)

    all_dates = date_generator.generate_initialization_dates(init_dates_config_key)
    existing_times = _get_existing_zarr_times(zarr_path)

    pending = [d for d in all_dates if not _is_date_already_done(d, existing_times)]
    n_skipped = len(all_dates) - len(pending)

    logger.info(
        f"Batch run starting: {len(all_dates)} total initialization dates, "
        f"{n_skipped} already done (skipped), {len(pending)} pending. "
        f"zarr={zarr_path}, variable_set='{variable_set}', n_workers={n_workers}"
    )

    n_success = 0
    n_failed = 0

    log_queue = multiprocessing.Queue(-1)
    queue_listener = logging.handlers.QueueListener(log_queue, *logging.getLogger().handlers)
    queue_listener.start()

    try:
        with ProcessPoolExecutor(
            max_workers=n_workers,
            initializer=_worker_logging_init,
            initargs=(log_queue,),
        ) as executor:
            futures = {
                executor.submit(
                    _process_one_date_worker, model_date, area_key, grid_key, paths_key, variable_set
                ): model_date
                for model_date in pending
            }

            for future in as_completed(futures):
                model_date = futures[future]
                result = future.result()

                if result[0] == "success":
                    _, _, dataset, downloaded_paths = result
                    try:
                        zarr_writer.write_zarr(dataset, zarr_path)
                        for path in downloaded_paths.values():
                            Path(path).unlink(missing_ok=True)
                        logger.info(f"SUCCESS {model_date}: appended to zarr, cleaned up GRIB files")
                        n_success += 1
                    except Exception as e:
                        logger.error(
                            f"FAILED {model_date}: zarr write/cleanup error: {e}. "
                            f"GRIB files kept: {list(downloaded_paths.values())}"
                        )
                        n_failed += 1
                else:
                    _, _, error_message = result
                    logger.error(f"FAILED {model_date}: {error_message}")
                    n_failed += 1
    finally:
        queue_listener.stop()

    logger.info(
        f"Batch run complete: {n_success} succeeded, {n_skipped} skipped, {n_failed} failed"
    )
    if n_failed > 0:
        logger.info("Re-run the same command to retry failed/remaining dates (resume-safe).")

    return {"succeeded": n_success, "skipped": n_skipped, "failed": n_failed}


def _setup_main_logging(paths_key):
    """Same single-shared-log-file setup as download_single_date.py."""
    log_path = config_loader.get_log_path(paths_key)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

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
    p = argparse.ArgumentParser(description="Batch download+convert S2S real-time forecast data across all initialization dates")
    p.add_argument("--init-dates-config-key", required=True,
                    help="Named model initialization dates config key (e.g. s2s_realtime_2025)")
    p.add_argument("--paths-key", required=True,
                    help="Named paths config key in config_paths.json (e.g. s2s_realtime_2025)")
    p.add_argument("--area-key", default="south_asia", help="Named area config key (default: south_asia)")
    p.add_argument("--grid-key", default="grids_1deg", help="Named grid config key (default: grids_1deg)")
    p.add_argument("--variable-set", default="combination_1",
                    help="Named variable set key in config_variables.json (default: combination_1)")
    p.add_argument("--n-workers", type=int, default=4, help="Number of parallel date workers (default: 4)")
    return p.parse_args()


def main():
    args = _parse_args()
    log_path = _setup_main_logging(args.paths_key)

    try:
        run_batch_download(
            args.init_dates_config_key,
            args.paths_key,
            area_key=args.area_key,
            grid_key=args.grid_key,
            variable_set=args.variable_set,
            n_workers=args.n_workers,
        )
    finally:
        logger.info(f"Log written to {log_path}")


if __name__ == "__main__":
    main()
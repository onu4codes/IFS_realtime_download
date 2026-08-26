#!/usr/bin/env python3
"""
sort_zarr_by_time.py

Sorts the main zarr store's 'time' (init_date) axis ascending, writing
the result to its configured sorted counterpart (config_paths.json ->
zarr_store_sorted).

Needed because batch_download.py appends one model-version-date's
block of hindcast years at a time, in the order dates were processed
-- not in globally ascending time order. E.g. block for 2025-01-01
(hyears 2005..2024) gets appended before block for 2025-01-03, so
2024-01-01 ends up sitting right before 2005-01-03 in the raw store.

Always OVERWRITES the sorted output on each run (rather than requiring
it not already exist) -- since the source store may have grown with
new dates since the last sort, the sorted version needs regenerating
fresh each time this runs, not left stale from a prior partial run.

Given the expected dataset size (a few GB for a full year at this
grid/lead-time/variable count), this loads the full dataset into
memory to sort. If the dataset grows much larger than that in the
future, this will need to switch to a chunked/out-of-core sort.

Per project convention: nothing is printed to the terminal. All
progress/errors go to the single shared pipeline log file.

CLI usage:
    python sort_zarr_by_time.py
    python sort_zarr_by_time.py --paths-key default --chunk-time 20
"""

import argparse
import logging
from pathlib import Path

import xarray as xr

from utils import config_loader

logger = logging.getLogger(__name__)


def sort_zarr(paths_key="default", chunk_time=20):
    """
    Sort the main zarr store's time axis ascending, writing the result
    to its configured sorted counterpart. Returns True if sorting was
    actually needed, False if the source was already sorted (sorted
    output is still written either way, for consistency).
    """
    input_path = config_loader.get_zarr_path(paths_key)
    output_path = config_loader.get_zarr_sorted_path(paths_key)

    if not input_path.exists():
        raise FileNotFoundError(f"Source zarr store not found: {input_path}")

    logger.info(f"Opening {input_path}")
    ds = xr.open_zarr(input_path)
    n_time = ds.sizes["time"]

    is_sorted = bool((ds["time"].values[:-1] <= ds["time"].values[1:]).all())
    logger.info(f"Loaded {n_time} time steps. Already sorted: {is_sorted}")

    logger.info("Sorting by time (loading into memory)...")
    ds_sorted = ds.sortby("time").load()

    # Clear chunk encoding inherited from the source store -- otherwise
    # xarray complains that the new dask chunking (below) conflicts with
    # the old on-disk chunk shape.
    for var in ds_sorted.variables:
        ds_sorted[var].encoding.pop("chunks", None)

    ds_sorted = ds_sorted.chunk({"time": chunk_time})

    if output_path.exists():
        logger.info(f"Sorted output already exists at {output_path} -- overwriting with fresh sort")
    else:
        logger.info(f"Writing sorted dataset to {output_path}")
    ds_sorted.to_zarr(output_path, mode="w")

    verify = xr.open_zarr(output_path)
    still_sorted = bool((verify["time"].values[:-1] <= verify["time"].values[1:]).all())
    logger.info(
        f"Sort complete. Verified ascending: {still_sorted}. "
        f"Total time steps: {verify.sizes['time']}. "
        f"First: {verify['time'].values[0]}. Last: {verify['time'].values[-1]}."
    )

    if not still_sorted:
        raise RuntimeError(
            f"Sorted output at {output_path} failed verification -- 'time' is not ascending. "
            f"Do not treat this output as reliable; investigate before using it."
        )

    return not is_sorted


def _setup_logging():
    """Same single-shared-log-file setup as the other top-level scripts."""
    log_path = config_loader.get_log_path()
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
    p = argparse.ArgumentParser(description="Sort the main S2S zarr store's time axis ascending")
    p.add_argument("--paths-key", default="default", help="Named paths config key (default: default)")
    p.add_argument("--chunk-time", type=int, default=20, help="Chunk size along time in the sorted output (default: 20)")
    return p.parse_args()


def main():
    args = _parse_args()
    log_path = _setup_logging()

    try:
        sort_zarr(paths_key=args.paths_key, chunk_time=args.chunk_time)
    finally:
        logger.info(f"Log written to {log_path}")


if __name__ == "__main__":
    main()
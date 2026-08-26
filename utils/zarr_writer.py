"""
utils/zarr_writer.py

Loads each variable group's downloaded GRIB file, standardizes it
(rename lat/lon, convert step to integer day-index, drop scalar cruft
coords), runs it through postprocess.apply_postprocess(), applies
output_names renaming (for groups where postprocess doesn't already
handle naming -- see note below), merges all groups into one dataset
for a model version date, and writes/appends the result to the target
Zarr store.

Naming note: 'flatten_pressure_levels' does both the flatten AND the
renaming internally (see postprocess.py docstring), since the two are
inherently coupled for pressure-level groups. All other postprocess
types (None, 'deaccumulate', 'realign_step_range_end_labeled') leave
variable names as cfgrib shortNames, so this module applies
output_names renaming for those afterward.

No printing -- uses the standard logging module, consistent with the
rest of utils/.
"""

import logging
from pathlib import Path

import numpy as np
import xarray as xr

from utils import config_loader
from utils import postprocess as pp
from utils.config_loader import ConfigError

logger = logging.getLogger(__name__)

# Groups whose postprocess function already handles output renaming
# internally -- skip the separate output_names rename step for these.
_POSTPROCESS_HANDLES_OWN_NAMING = {"flatten_pressure_levels"}

_SCALAR_COORDS_TO_DROP = {
    "number", "heightAboveGround", "meanSea", "valid_time", "surface",
}


def group_filename(group_name, model_date):
    """
    Shared file naming convention for one group's downloaded GRIB file.
    Used by both the download script and this module, so they always
    agree on where to find/write a given group's file.
    """
    return f"s2s_{group_name}_{model_date:%Y%m%d}.grib"


def load_group(path):
    """Open one group's GRIB file via cfgrib."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"GRIB file not found: {path}")
    return xr.open_dataset(path, engine="cfgrib", backend_kwargs={"indexpath": ""})


def standardize_dims(ds):
    """Rename latitude/longitude -> lat/lon, convert step to integer day-index."""
    rename_map = {}
    if "latitude" in ds.dims or "latitude" in ds.coords:
        rename_map["latitude"] = "lat"
    if "longitude" in ds.dims or "longitude" in ds.coords:
        rename_map["longitude"] = "lon"
    if rename_map:
        ds = ds.rename(rename_map)

    if np.issubdtype(ds["step"].dtype, np.timedelta64):
        step_days = (ds["step"].values / np.timedelta64(1, "D")).astype(int)
        ds = ds.assign_coords(step=step_days)

    return ds


def drop_scalar_coords(ds):
    """Drop known non-dimension scalar coords we don't need downstream."""
    drop_these = [c for c in _SCALAR_COORDS_TO_DROP if c in ds.coords]
    return ds.drop_vars(drop_these, errors="ignore")


def process_group_file(path, group_name, variable_set="combination_1"):
    """
    Full pipeline for one group's GRIB file: load, standardize, drop
    scalar cruft, apply postprocess, apply output_names renaming
    (unless postprocess already handled naming).

    Defaults to variable_set='combination_1' so existing calls that
    don't pass this parameter keep working unchanged.
    """
    group_config = config_loader.get_variable_group(group_name, variable_set=variable_set)

    ds = load_group(path)
    ds = standardize_dims(ds)
    ds = drop_scalar_coords(ds)
    ds = pp.apply_postprocess(ds, group_config)

    postprocess_name = group_config.get("postprocess")
    if postprocess_name not in _POSTPROCESS_HANDLES_OWN_NAMING:
        output_names = group_config["output_names"]
        rename_map = {k: v for k, v in output_names.items() if k in ds.data_vars}
        ds = ds.rename(rename_map)

    logger.info(f"Processed group '{group_name}' (set='{variable_set}') from {path} -> vars {list(ds.data_vars)}")
    return ds


def _get_target_steps(group_names, variable_set="combination_1"):
    """
    Derive the common step axis (0..max_day) from the variable groups'
    own leadtime config, asserting all groups agree on the same range.
    """
    max_days = set()
    for group_name in group_names:
        group_config = config_loader.get_variable_group(group_name, variable_set=variable_set)
        max_days.add(group_config["leadtime_end"] // group_config["leadtime_step"])

    if len(max_days) != 1:
        raise ConfigError(
            f"Variable groups disagree on max lead day: {max_days}. "
            f"All groups must share the same leadtime_end/leadtime_step ratio."
        )
    max_day = max_days.pop()
    return np.arange(0, max_day + 1)


def build_dataset(group_file_map, model_date, variable_set="combination_1"):
    """
    Build the full merged dataset for one model version date.

    group_file_map: dict of {group_name: grib_file_path}
    model_date: datetime.date for this model version date

    Defaults to variable_set='combination_1' so existing calls that
    don't pass this parameter keep working unchanged.

    Returns the merged xarray.Dataset with dims (time, step, lat, lon),
    ready to write/append to the target Zarr store.
    """
    processed = {
        group_name: process_group_file(path, group_name, variable_set=variable_set)
        for group_name, path in group_file_map.items()
    }

    merged = xr.merge(list(processed.values()), join="outer")

    target_steps = _get_target_steps(group_file_map.keys(), variable_set=variable_set)
    merged = merged.reindex(step=target_steps)

    merged.attrs.update({
        "description": f"S2S IFS reforecast, model version date {model_date:%Y-%m-%d}, variable_set={variable_set}",
        "n_lead_time": int(target_steps.max()),
        "step_range": f"0 to {int(target_steps.max())}",
    })

    logger.info(
        f"Built merged dataset for model_date={model_date} (set='{variable_set}'): "
        f"vars={list(merged.data_vars)}, dims={dict(merged.sizes)}"
    )
    return merged


def write_zarr(ds, zarr_path, append_dim="time"):
    """Write a new zarr store, or append to an existing one along append_dim."""
    zarr_path = Path(zarr_path)
    if zarr_path.exists():
        logger.info(f"Appending to existing zarr store at {zarr_path} along '{append_dim}'")
        ds.to_zarr(zarr_path, mode="a", append_dim=append_dim)
    else:
        logger.info(f"Creating new zarr store at {zarr_path}")
        ds.to_zarr(zarr_path, mode="w")
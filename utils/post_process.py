"""
utils/postprocess.py

Per-group postprocessing transforms, dispatched by each variable
group's 'postprocess' value in config_variables.json:

    null                                -> no-op
    "deaccumulate"                      -> total_precipitation
    "realign_step_range_end_labeled"    -> 2m_temperature
    "flatten_pressure_levels"           -> pressure_level_vars

PRECONDITION: all functions here assume the dataset has already been
through basic standardization, done upstream in zarr_writer.py:
    - 'latitude'/'longitude' renamed to 'lat'/'lon'
    - 'step' converted from timedelta64[ns] to integer day-index
This module does not do that standardization itself -- it only holds
the group-specific transforms that run after it.

No file I/O, no printing -- uses the standard logging module,
consistent with the rest of utils/.
"""

import logging

import numpy as np
import xarray as xr

from utils.config_loader import ConfigError

logger = logging.getLogger(__name__)


def deaccumulate(ds, group_config):
    """
    Deaccumulate a cumulative-since-forecast-start field (e.g. total
    precipitation) into true per-day totals.

    day_N = value[step=N] - value[step=N-1], with day_0 left as-is
    (should be 0 by construction, since nothing has accumulated yet).

    Applies to every data variable present in ds.
    """
    out = ds.copy()
    for var_name in list(ds.data_vars):
        da = ds[var_name]
        diffed = da.diff(dim="step")
        day0 = da.isel(step=0).expand_dims(step=[da["step"].values[0]])
        combined = xr.concat([day0, diffed], dim="step")
        combined["step"] = da["step"]  # restore original step coord (diff shifts it)

        # Preserve original dim order (diff/concat can reorder dims).
        combined = combined.transpose(*da.dims)
        out[var_name] = combined

    logger.info(f"Deaccumulated {list(ds.data_vars)}")
    return out


def realign_step_range_end_labeled(ds, group_config):
    """
    Realign a period-mean field whose native 'step' values are END-labeled
    (e.g. 2m_temperature requested as "0_24", "24_48", ... -> native step
    values 1, 2, ..., N+1 in days) onto the common 0..target_max_day axis.

    target_max_day is derived from the group's own leadtime config
    (leadtime_end // leadtime_step), so it always matches the other
    (point-type) groups' step range without needing a separate constant.

    Drops the trailing step that falls outside the target range, then
    reindexes onto 0..target_max_day -- step=0 becomes NaN for this
    variable, since there is no valid "day 0 mean" (day 0 is the
    initialization instant, not a 24h window).
    """
    target_max_day = group_config["leadtime_end"] // group_config["leadtime_step"]
    target_steps = np.arange(0, target_max_day + 1)

    out = ds.sel(step=ds["step"] <= target_max_day)
    out = out.reindex(step=target_steps)

    logger.info(
        f"Realigned {list(ds.data_vars)} onto step 0..{target_max_day} "
        f"(step=0 will be NaN for this group)"
    )
    return out


def flatten_pressure_levels(ds, group_config):
    """
    Flatten a pressure-level dimension into separate named variables,
    one per (data variable, level) combination, e.g.:
        q (isobaricInhPa: 1000,850,500,200) ->
            specific_humidity_1000, specific_humidity_850, ...

    Final variable names come from group_config['output_names'], keyed
    by the cfgrib shortName (e.g. 'q' -> 'specific_humidity'), combined
    with the integer level value. This function does both the flatten
    AND the renaming for pressure-level groups, since the two are
    inherently coupled here (unlike single-level groups, where renaming
    is a simple 1:1 rename done separately in zarr_writer.py).

    Auto-detects the level dimension as whichever dim isn't one of
    the standard (time, step, lat, lon).
    """
    standard_dims = {"time", "step", "lat", "lon"}
    level_dims = [d for d in ds.dims if d not in standard_dims]
    if len(level_dims) != 1:
        raise ConfigError(
            f"Expected exactly one pressure-level dimension, found {level_dims} "
            f"in dataset with dims {list(ds.dims)}"
        )
    level_dim = level_dims[0]

    output_names = group_config["output_names"]
    out = xr.Dataset()

    for var_name in ds.data_vars:
        if var_name not in output_names:
            raise ConfigError(
                f"No output_names entry for shortName '{var_name}'. "
                f"Available: {sorted(output_names.keys())}"
            )
        full_name = output_names[var_name]
        for level in ds[level_dim].values:
            level_int = int(level)
            new_var_name = f"{full_name}_{level_int}"
            out[new_var_name] = ds[var_name].sel({level_dim: level}, drop=True)

    # Carry over the remaining coords (time, step, lat, lon), dropping the
    # now-flattened level dimension.
    out = out.assign_coords({k: v for k, v in ds.coords.items() if k != level_dim})

    logger.info(f"Flattened {list(ds.data_vars)} over '{level_dim}' -> {list(out.data_vars)}")
    return out


# Lookup table: postprocess name (as used in config_variables.json) -> function.
_POSTPROCESS_HANDLERS = {
    "deaccumulate": deaccumulate,
    "realign_step_range_end_labeled": realign_step_range_end_labeled,
    "flatten_pressure_levels": flatten_pressure_levels,
}


def apply_postprocess(ds, group_config):
    """
    Main dispatch function. Looks up group_config['postprocess'] and
    calls the matching handler, or returns ds unchanged if it's null.
    """
    postprocess_name = group_config.get("postprocess")

    if postprocess_name is None:
        return ds

    handler = _POSTPROCESS_HANDLERS.get(postprocess_name)
    if handler is None:
        raise ConfigError(
            f"Unknown postprocess '{postprocess_name}'. "
            f"Implemented: {sorted(_POSTPROCESS_HANDLERS.keys())}"
        )

    return handler(ds, group_config)
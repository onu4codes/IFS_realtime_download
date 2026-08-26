"""
utils/config_loader.py

Single access point for all 5 config JSON files:
    config/config_area.json
    config/config_grid.json
    config/config_variables.json
    config/config_model_initialization_dates.json
    config/config_paths.json

Every other module (date_generator, request_builder, postprocess,
zarr_writer) and every top-level script should import from here rather
than reading config/*.json directly or hardcoding paths.

Adapted from the reforecast pipeline's config_loader.py for the
real-time pipeline. Two differences from the reforecast version:

    1. config_model_version_dates.json -> config_model_initialization_dates.json
       (renamed throughout: file, constant, load/get functions) --
       real-time forecasts have an "initialization date" (the day the
       forecast was run), not a "model version date" tied to a
       hindcast archive.
    2. 'hindcast_years_back' is no longer a required (or recognized)
       key in config_model_initialization_dates.json entries, since
       real-time forecasts have no hindcast expansion at all.

Validates structure on load so mistakes (missing keys, typos) surface
immediately with a clear error, rather than failing deep inside a
multi-hour batch run.

This module does not print to the terminal -- it uses the standard
logging module with no handlers attached, so top-level scripts can
attach a file handler (or any handler) without this module needing
changes.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
REPO_ROOT = CONFIG_DIR.parent

AREA_CONFIG_FILE = CONFIG_DIR / "config_area.json"
GRID_CONFIG_FILE = CONFIG_DIR / "config_grid.json"
VARIABLES_CONFIG_FILE = CONFIG_DIR / "config_variables.json"
MODEL_INIT_DATES_CONFIG_FILE = CONFIG_DIR / "config_model_initialization_dates.json"
PATHS_CONFIG_FILE = CONFIG_DIR / "config_paths.json"

# Required keys for each named entry, per config file.
_AREA_REQUIRED_KEYS = {"area"}
_GRID_REQUIRED_KEYS = {"grid"}
_VARIABLES_REQUIRED_KEYS = {
    "cds_variable", "level_type", "level_value",
    "leadtime_type", "leadtime_start", "leadtime_end", "leadtime_step",
    "output_names", "postprocess",
}
# No 'hindcast_years_back' here -- real-time initialization dates have
# no hindcast expansion, so that key is neither required nor read.
_MODEL_INIT_DATES_REQUIRED_KEYS = {
    "start_date", "end_date", "cadence",
}
_PATHS_REQUIRED_KEYS = {"zarr_store", "zarr_store_sorted", "workdir", "log_file"}


class ConfigError(Exception):
    """Raised for any problem loading or validating a config file."""


def _load_json(path):
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigError(f"Invalid JSON in {path}: {e}") from e
    if not isinstance(data, dict):
        raise ConfigError(f"Expected a JSON object (dict) at top level of {path}, got {type(data).__name__}")
    return data


def _validate_entries(data, required_keys, source_path):
    for entry_name, entry in data.items():
        if not isinstance(entry, dict):
            raise ConfigError(
                f"Entry '{entry_name}' in {source_path} must be an object, got {type(entry).__name__}"
            )
        missing = required_keys - entry.keys()
        if missing:
            raise ConfigError(
                f"Entry '{entry_name}' in {source_path} is missing required keys: {sorted(missing)}"
            )


def load_area_config():
    """Load and validate config_area.json. Returns the full dict."""
    data = _load_json(AREA_CONFIG_FILE)
    _validate_entries(data, _AREA_REQUIRED_KEYS, AREA_CONFIG_FILE)
    logger.info(f"Loaded area config: {list(data.keys())}")
    return data


def load_grid_config():
    """Load and validate config_grid.json. Returns the full dict."""
    data = _load_json(GRID_CONFIG_FILE)
    _validate_entries(data, _GRID_REQUIRED_KEYS, GRID_CONFIG_FILE)
    logger.info(f"Loaded grid config: {list(data.keys())}")
    return data


def load_variables_config():
    """
    Load and validate config_variables.json. Returns the full dict.

    Structure is nested: {variable_set_name: {group_name: {group_config}}}.
    Each group_config within every variable set is validated against
    _VARIABLES_REQUIRED_KEYS.
    """
    data = _load_json(VARIABLES_CONFIG_FILE)

    for set_name, groups in data.items():
        if not isinstance(groups, dict):
            raise ConfigError(
                f"Variable set '{set_name}' in {VARIABLES_CONFIG_FILE} must be an object "
                f"of group_name -> group_config, got {type(groups).__name__}"
            )
        _validate_entries(groups, _VARIABLES_REQUIRED_KEYS, f"{VARIABLES_CONFIG_FILE} (set '{set_name}')")

    logger.info(f"Loaded variables config, sets: {list(data.keys())}")
    return data


def load_model_init_dates_config():
    """Load and validate config_model_initialization_dates.json. Returns the full dict."""
    data = _load_json(MODEL_INIT_DATES_CONFIG_FILE)
    _validate_entries(data, _MODEL_INIT_DATES_REQUIRED_KEYS, MODEL_INIT_DATES_CONFIG_FILE)
    logger.info(f"Loaded model initialization dates config: {list(data.keys())}")
    return data


def load_paths_config():
    """Load and validate config_paths.json. Returns the full dict."""
    data = _load_json(PATHS_CONFIG_FILE)
    _validate_entries(data, _PATHS_REQUIRED_KEYS, PATHS_CONFIG_FILE)
    logger.info(f"Loaded paths config: {list(data.keys())}")
    return data


def _lookup(data, key, config_label):
    if key not in data:
        raise ConfigError(
            f"Unknown {config_label} key '{key}'. Available: {sorted(data.keys())}"
        )
    return data[key]


def get_area(key):
    """Return the 'area' list for a named area config, e.g. get_area('south_asia')."""
    data = load_area_config()
    entry = _lookup(data, key, "area")
    return entry["area"]


def get_grid(key):
    """Return the 'grid' list for a named grid config, e.g. get_grid('grids_1deg')."""
    data = load_grid_config()
    entry = _lookup(data, key, "grid")
    return entry["grid"]


def get_variable_group(group_name, variable_set="combination_1"):
    """
    Return the full config dict for one variable group within a named
    variable set, e.g. get_variable_group('precip', variable_set='rainfall_only').

    Defaults to variable_set='combination_1' so existing calls that don't
    pass this parameter keep working unchanged.
    """
    data = load_variables_config()
    groups = _lookup(data, variable_set, "variable set")
    return _lookup(groups, group_name, f"variable group (in set '{variable_set}')")


def get_model_init_dates_config(key):
    """
    Return the full config dict for one named initialization-dates
    config entry, e.g. get_model_init_dates_config('s2s_realtime_2025').
    """
    data = load_model_init_dates_config()
    return _lookup(data, key, "model initialization dates")


def get_zarr_path(key):
    """
    Return the absolute Path to the target zarr store for a named
    paths config, e.g. get_zarr_path('s2s_realtime_2025').

    Paths in config_paths.json are given relative to the repo root
    (the parent directory of config/ and utils/), and are resolved
    here to an absolute path regardless of the caller's working
    directory.
    """
    data = load_paths_config()
    entry = _lookup(data, key, "paths")
    return (REPO_ROOT / entry["zarr_store"]).resolve()


def get_zarr_sorted_path(key):
    """
    Return the absolute Path to the time-sorted counterpart of the
    main zarr store, e.g. get_zarr_sorted_path('s2s_realtime_2025').
    Same repo-root-relative resolution as get_zarr_path().
    """
    data = load_paths_config()
    entry = _lookup(data, key, "paths")
    return (REPO_ROOT / entry["zarr_store_sorted"]).resolve()


def get_workdir_path(key):
    """
    Return the absolute Path to the scratch download directory for a
    named paths config, e.g. get_workdir_path('s2s_realtime_2025'). Same
    repo-root-relative resolution as get_zarr_path().
    """
    data = load_paths_config()
    entry = _lookup(data, key, "paths")
    return (REPO_ROOT / entry["workdir"]).resolve()


def get_log_path(key):
    """
    Return the absolute Path to the single shared pipeline log file
    for a named paths config, e.g. get_log_path('s2s_realtime_2025').
    Same repo-root-relative resolution as get_zarr_path()/get_workdir_path().

    This is the ONE log file for a given paths_key -- all top-level
    scripts and utils/ modules funnel their log records here via the
    standard logging propagation to the root logger, so there is a
    single continuous record of a whole batch run, not one file per
    date or per script.
    """
    data = load_paths_config()
    entry = _lookup(data, key, "paths")
    return (REPO_ROOT / entry["log_file"]).resolve()


def list_variable_groups(variable_set="combination_1"):
    """
    Return the list of all variable group names within a named variable
    set, e.g. list_variable_groups('rainfall_only') -> ['precip'].

    Defaults to variable_set='combination_1' so existing calls that don't
    pass this parameter keep working unchanged.
    """
    data = load_variables_config()
    groups = _lookup(data, variable_set, "variable set")
    return list(groups.keys())


def list_variable_sets():
    """Return the list of all named variable set names defined in config_variables.json."""
    return list(load_variables_config().keys())
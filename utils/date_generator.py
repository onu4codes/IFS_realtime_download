"""
utils/date_generator.py

Generates the list of forecast initialization dates from a named entry
in config_model_initialization_dates.json.

Cadence rules are implemented as a lookup table of functions, keyed by
the 'cadence' string in the config -- adding a new cadence later means
adding one function and one dict entry here, not touching any calling
code.

Adapted from the reforecast pipeline's date_generator.py for the
real-time pipeline:
    - No hindcast-year concept at all. The reforecast version's
      get_hindcast_years() / get_model_dates_with_hindcast_years()
      have no equivalent here -- real-time forecasts have exactly one
      init date, not an init date paired with a list of hindcast years.
      generate_initialization_dates() is the only entry point callers
      need.
    - Two cadences implemented instead of one: 'monday_thursday' (ECMWF's
      real-time S2S schedule before it moved to daily runs) and
      'all_dates' (every calendar day -- the schedule after the switch
      to daily runs). See config_model_initialization_dates.json's
      per-entry 'description' field for which cadence applies to which
      date range, and verify against ECMWF's S2S real-time schedule
      documentation before adding a new date range that straddles the
      cutover.

No file I/O beyond what config_loader already does, no printing --
uses the standard logging module, consistent with the rest of utils/.
"""
import logging
from datetime import datetime, timedelta
from utils import config_loader
from utils.config_loader import ConfigError
logger = logging.getLogger(__name__)


def _cadence_monday_thursday(start_date, end_date):
    """All Mondays and Thursdays in range, inclusive of both ends."""
    dates = []
    d = start_date
    while d <= end_date:
        if d.weekday() in (0, 3):  # Monday=0, Thursday=3
            dates.append(d)
        d += timedelta(days=1)
    return dates


def _cadence_all_dates(start_date, end_date):
    """Every calendar day in range, inclusive of both ends."""
    dates = []
    d = start_date
    while d <= end_date:
        dates.append(d)
        d += timedelta(days=1)
    return dates


# Lookup table: cadence name (as used in config_model_initialization_dates.json)
# -> function.
_CADENCE_HANDLERS = {
    "monday_thursday": _cadence_monday_thursday,
    "all_dates": _cadence_all_dates,
}


def _parse_date(date_str, field_name):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ConfigError(f"Invalid {field_name} '{date_str}', expected YYYY-MM-DD") from e


def generate_initialization_dates(config_key):
    """
    Return the list of forecast initialization dates (datetime.date
    objects) for the named config entry, per its 'cadence' rule.

    This is the main entry point for callers (e.g. batch_download.py)
    -- unlike the reforecast pipeline, there is no hindcast-year
    pairing step; each date returned here corresponds to exactly one
    real-time forecast run.
    """
    cfg = config_loader.get_model_init_dates_config(config_key)
    start_date = _parse_date(cfg["start_date"], "start_date")
    end_date = _parse_date(cfg["end_date"], "end_date")
    if start_date > end_date:
        raise ConfigError(
            f"start_date {start_date} is after end_date {end_date} in "
            f"model initialization dates config '{config_key}'"
        )
    cadence = cfg["cadence"]
    handler = _CADENCE_HANDLERS.get(cadence)
    if handler is None:
        raise ConfigError(
            f"Unknown cadence '{cadence}' in model initialization dates config '{config_key}'. "
            f"Implemented cadences: {sorted(_CADENCE_HANDLERS.keys())}"
        )
    dates = handler(start_date, end_date)
    logger.info(
        f"Generated {len(dates)} initialization dates for config '{config_key}' "
        f"(cadence='{cadence}', {start_date} to {end_date})"
    )
    return dates
"""
config/observability.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-OBS-001, SPEC-OBS-002, SPEC-OBS-003, SPEC-OBS-005
Owner: Platform / Observability
Consumers: ingestion/quality/structured_logger, ingestion/scheduler, features,
    systems/ml_signal_engine

Master observability switch (SPEC-OBS-001): every module that emits
structured logs or metrics gates on this module first, never on
config.settings.OBSERVABILITY_ENABLED/OBSERVABILITY_LEVEL directly, so the
on/off and verbosity policy has exactly one home.

NOTE on level vocabulary: this task's instructions describe
OBSERVABILITY_LEVEL as 'production' | 'development' | 'debug'. The actual
spec (SPEC-OBS-002) defines five levels — 'off' | 'error' | 'warning' |
'info' | 'debug' — already implemented as config.settings.OBSERVABILITY_LEVEL,
and SPEC-OBS-005 explicitly defines "production" as the two least-verbose
of those five ("In production (OBSERVABILITY_LEVEL='error' or 'warning')").
This module follows the spec (the more detailed, more precisely defined
source) rather than the task's three-way paraphrase, and exposes
is_production_mode() as the derived boolean the task's wording was
gesturing at. Same resolution pattern as ingestion/adjust/price_adjuster.py's
SPLIT-direction note.
"""

import json
import logging

from config.settings import OBSERVABILITY_ENABLED, OBSERVABILITY_LEVEL, OBSERVABILITY_LOG_PATH
from config.timezone import now_ist

logger = logging.getLogger(__name__)

# SPEC-OBS-002: ordered least -> most verbose. Index = verbosity rank.
LEVELS = ["off", "error", "warning", "info", "debug"]

if OBSERVABILITY_LEVEL not in LEVELS:
    raise ValueError(f"Unknown OBSERVABILITY_LEVEL '{OBSERVABILITY_LEVEL}'. Must be one of {LEVELS}.")

# SPEC-OBS-005: "In production (OBSERVABILITY_LEVEL='error' or 'warning')".
_PRODUCTION_LEVELS = {"error", "warning"}


def is_enabled() -> bool:
    """
    SPEC-OBS-001: the master switch. False means zero observability overhead
    anywhere in the system (NoOpObservability everywhere).
    """
    return OBSERVABILITY_ENABLED


def is_production_mode() -> bool:
    """
    SPEC-OBS-005: production mode is OBSERVABILITY_LEVEL in {'error', 'warning'}.
    In production mode: no per-stock logging, no debug-level metrics, no
    intermediate file writes (see allow_intermediate_file_write()).
    """
    return OBSERVABILITY_LEVEL in _PRODUCTION_LEVELS


def should_log(event_level: str) -> bool:
    """
    Whether an event at `event_level` should be emitted, given the master
    switch and the configured OBSERVABILITY_LEVEL.

    Parameters
    ----------
    event_level : str
        One of LEVELS (excluding 'off', which is a configuration value,
        not a valid event severity).

    Returns
    -------
    bool
        False if observability is disabled, if OBSERVABILITY_LEVEL is
        'off', or if event_level is less severe (more verbose) than the
        configured level. True otherwise — e.g. an 'error' event is always
        logged whenever observability is enabled and level != 'off'.

    Spec References
    ----------------
    SPEC-OBS-002: level ordering and inclusion semantics ("'warning': +
    data quality warnings, drift alerts" — i.e. each level includes
    everything less verbose than it).

    Raises
    ------
    ValueError
        If event_level is 'off' or not a recognized level.
    """
    if event_level not in LEVELS or event_level == "off":
        raise ValueError(f"Invalid event_level '{event_level}'. Must be one of {LEVELS[1:]}.")
    if not OBSERVABILITY_ENABLED or OBSERVABILITY_LEVEL == "off":
        return False
    return LEVELS.index(event_level) <= LEVELS.index(OBSERVABILITY_LEVEL)


def allow_intermediate_file_write() -> bool:
    """
    SPEC-OBS-005: "In production mode: no verbose logging, no intermediate
    file writes." Gate any debug-only artifact write (e.g. per-step
    intermediate Parquet/CSV dumps used for local debugging) on this.

    Returns
    -------
    bool
        False when is_production_mode() is True or observability is
        disabled; True otherwise.
    """
    return OBSERVABILITY_ENABLED and not is_production_mode()


class NoOpObservability:
    """
    SPEC-OBS-001: "When off: zero performance overhead." Every method is a
    no-op so callers can unconditionally call observability methods without
    branching on is_enabled() at every call site.
    """

    def log_event(self, event_type: str, level: str = "info", **fields) -> None:
        return None


class JSONLObservability:
    """
    SPEC-OBS-003: emits JSON-line events to OBSERVABILITY_LOG_PATH
    (datastore/logs/observability.jsonl), gated per-event by should_log().
    This is the general event stream; ingestion/quality/structured_logger.py
    is a separate, narrower emitter for per-pipeline-step events into their
    own daily-rotated file (SPEC-OBS-003's "Log rotation: daily" applies
    there, not here — this file is a single rolling stream).
    """

    def log_event(self, event_type: str, level: str = "info", **fields) -> None:
        """
        Append one JSON-line event, if should_log(level) permits it.

        Parameters
        ----------
        event_type : str
            e.g. 'step_start', 'step_complete', 'drift_alert'.
        level : str
            Event severity — one of LEVELS (excluding 'off'). Default 'info'.
        **fields :
            Extra event-specific fields (step_id, duration, rows_processed, ...).
            SPEC-SEC-001: callers must never pass raw financial data values here.

        Returns
        -------
        None
        """
        if not should_log(level):
            return
        event = {
            "event_type": event_type,
            "level": level,
            "timestamp": now_ist().isoformat(),
            **fields,
        }
        OBSERVABILITY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OBSERVABILITY_LOG_PATH, "a") as f:
            f.write(json.dumps(event, default=str) + "\n")


def get_observability():
    """
    Factory: returns NoOpObservability when the master switch is off
    (SPEC-OBS-001: zero overhead when disabled), otherwise a
    JSONLObservability instance.

    Returns
    -------
    NoOpObservability or JSONLObservability
    """
    if not OBSERVABILITY_ENABLED:
        return NoOpObservability()
    return JSONLObservability()

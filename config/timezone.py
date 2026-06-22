"""
config/timezone.py

Phase: 1.4 (Labeling + Backtesting Infrastructure follow-up)
Specs: SPEC-QUALITY-003
Owner: Platform / DataStore
Consumers: ingestion/quality/structured_logger, datastore/api/main,
           ingestion/scheduler/*, scripts/*, config/observability

Single source of truth for "what time/date is it" across AlphaLens. This
is an India-only system (NSE trading hours, daily pipeline windows,
operator-facing reports) — every timestamp that gets logged, displayed,
or used to name a file must be IST (Asia/Kolkata), never naive local time
(which silently means "whatever timezone the host OS happens to be set
to") and never UTC (correct for epoch/API math, wrong for "what day is it
for an India-based operator reading this log at 1 AM IST" — a real bug
found in ingestion/quality/structured_logger.py: it named log files by
UTC date while its own test computed the expected filename by OS-local
date, and they silently disagreed for the ~5.5 hours per day where IST
and UTC are on different calendar dates).

Use now_ist() (or IST directly with an explicit datetime) everywhere a
"current timestamp" is needed for logging/display/file-naming purposes.
Do NOT use this for parsing third-party API timestamps that are
contractually UTC-based (e.g. FYERS' unix-epoch candle timestamps,
ingestion/scrapers/fyers_backfill.py) — that conversion already correctly
goes UTC-epoch -> Asia/Kolkata internally and is a different concern from
this module.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    """
    Current timestamp in Asia/Kolkata, timezone-aware.

    Returns
    -------
    datetime
        Timezone-aware datetime with tzinfo=IST. Never naive, never UTC.

    Spec References
    ----------------
    SPEC-QUALITY-003: single source of truth for a project-wide constant
    (here, "what timezone is now") — no other module should call
    datetime.now()/datetime.utcnow() directly for a timestamp that gets
    logged, displayed, or used to name a file.

    Raises
    ------
    None
    """
    return datetime.now(IST)

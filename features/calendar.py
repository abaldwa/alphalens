"""
features/calendar.py

Phase: 1.1 (Core Feature Computation)
Specs: SPEC-FEAT-003, SPEC-PIPE-004
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine

Computes 7 calendar/seasonal features. These depend only on the calendar
date (never on a stock's price/volume history), so the same row applies
to every ticker on a given date — features/matrix_builder.py broadcasts
the single output row across the day's universe.

The only iteration in this module is over the small set of *unique dates*
requested (1 for a normal daily pipeline run; at most a few thousand for a
historical backfill) — never over stocks (SPEC-PIPE-004).
"""

import calendar as _calendar
import logging
from datetime import date as date_type
from datetime import timedelta
from functools import lru_cache
from typing import Iterable, Union

import numpy as np
import pandas as pd

from config.nse_holidays import is_nse_holiday

logger = logging.getLogger(__name__)

CALENDAR_FEATURES = [
    "month_sin",
    "month_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "is_expiry_week",
    "days_to_expiry",
    "quarter_end_proximity",
]

_QUARTER_END_MONTH_DAYS = [(3, 31), (6, 30), (9, 30), (12, 31)]
_EXPIRY_WEEKDAY = 3  # Thursday


@lru_cache(maxsize=512)
def _monthly_expiry(year: int, month: int) -> date_type:
    """
    NSE monthly F&O expiry: last Thursday of the month, rolled back to the
    nearest trading day if that Thursday is a weekend/holiday.

    Spec References
    ----------------
    SPEC-FEAT-003 (calendar features), config/nse_holidays.py (holiday list).
    """
    last_day = _calendar.monthrange(year, month)[1]
    d = date_type(year, month, last_day)
    d -= timedelta(days=(d.weekday() - _EXPIRY_WEEKDAY) % 7)
    while d.weekday() >= 5 or is_nse_holiday(d):
        d -= timedelta(days=1)
    return d


def _next_monthly_expiry(d: date_type) -> date_type:
    """The monthly expiry on or after `d` (this month's if not yet passed, else next month's)."""
    expiry = _monthly_expiry(d.year, d.month)
    if d <= expiry:
        return expiry
    next_year, next_month = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
    return _monthly_expiry(next_year, next_month)


def _trading_days_between(start: date_type, end: date_type) -> int:
    """Count trading days strictly after `start` through `end` inclusive."""
    if end <= start:
        return 0
    span = pd.date_range(start + timedelta(days=1), end, freq="D")
    return int(sum(1 for ts in span if ts.weekday() < 5 and not is_nse_holiday(ts.date())))


def _nearest_quarter_end_proximity(d: date_type) -> int:
    """Signed calendar days to the nearest quarter end (negative = already passed)."""
    candidates = [
        date_type(year, month, day)
        for year in (d.year - 1, d.year, d.year + 1)
        for month, day in _QUARTER_END_MONTH_DAYS
    ]
    return min(((c - d).days for c in candidates), key=abs)


def _one_date_row(d: date_type) -> dict:
    month_angle = 2 * np.pi * d.month / 12
    dow_angle = 2 * np.pi * (d.weekday() % 5) / 5  # trading week has 5 days (Mon-Fri)
    expiry = _next_monthly_expiry(d)
    return {
        "month_sin": np.sin(month_angle),
        "month_cos": np.cos(month_angle),
        "day_of_week_sin": np.sin(dow_angle),
        "day_of_week_cos": np.cos(dow_angle),
        "is_expiry_week": int(d.isocalendar()[:2] == expiry.isocalendar()[:2]),
        "days_to_expiry": _trading_days_between(d, expiry),
        "quarter_end_proximity": _nearest_quarter_end_proximity(d),
    }


def compute_calendar_features(dates: Union[Iterable, str, date_type]) -> pd.DataFrame:
    """
    Compute the 7 calendar features for one or more dates.

    Parameters
    ----------
    dates : str, date, or iterable of such
        Date(s) to compute features for. A single date is accepted for the
        common daily-pipeline case; an iterable supports historical backfills.

    Returns
    -------
    pd.DataFrame
        Columns: date (datetime64) + CALENDAR_FEATURES (7 cols), one row
        per unique input date, float64 except is_expiry_week/days_to_expiry
        (int) which are cast to float64 for matrix-assembly consistency.

    Spec References
    ----------------
    SPEC-FEAT-003: month_of_year and day_of_week are cyclically encoded as
    sin/cos pairs; raw integer month/day-of-week are never exposed.

    PIT Assumptions
    ----------------
    None — calendar facts are deterministic and knowable arbitrarily far in
    advance; there is no look-ahead risk in this module.

    Raises
    ------
    ValueError
        If `dates` is empty.
    """
    if isinstance(dates, (str, date_type, pd.Timestamp)):
        dates = [dates]
    unique_dates = pd.to_datetime(pd.Series(list(dates)).unique())
    if len(unique_dates) == 0:
        raise ValueError("dates must contain at least one date")

    rows = [{"date": ts, **_one_date_row(ts.date())} for ts in sorted(unique_dates)]
    out = pd.DataFrame(rows)
    for col in CALENDAR_FEATURES:
        out[col] = out[col].astype(np.float64)
    return out

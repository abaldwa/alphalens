"""
config/nse_holidays.py

Phase: 0
Specs: SPEC-SCHED-008
Owner: Platform / Scheduler
Consumers: ingestion/scheduler, datastore/api

NSE equity trading holiday calendar, consumed by the gap detector so that
non-trading days are excluded from backfill (SPEC-SCHED-003, SPEC-SCHED-008).
This file must be reviewed and updated annually, as soon as NSE/BSE publish
the following year's official trading holiday circular.

IMPORTANT — accuracy note for 2026:
NSE/BSE typically publish next year's holiday circular in December. As of the
last update to this file, the 2026 circular had not yet been published. The
holidays below for 2026 are split into two groups:
  1. Fixed-date national holidays (Republic Day, Ambedkar Jayanti, Maharashtra
     Day, Gandhi Jayanti, Christmas) and Good Friday (computable from the
     ecclesiastical Easter algorithm) — these are included with high confidence.
  2. Lunar/festival-based holidays (Mahashivratri, Holi, Ram Navami / Mahavir
     Jayanti, Id-Ul-Fitr, Ganesh Chaturthi, Dussehra, Diwali Laxmi Pujan,
     Diwali Balipratipada, Guru Nanak Jayanti, Bakri Id, Muharram) shift every
     year and are NOT included for 2026 — see NSE_HOLIDAYS_2026_PENDING below.

A missing lunar holiday is a soft failure: the gap detector will attempt to
backfill that date, find no bhavcopy published, and log it as a missing file —
it will not corrupt data. Still, this list MUST be completed from the official
NSE circular before relying on 2026 backfill/scheduling in production.
"""

from datetime import date

# ---------------------------------------------------------------------------
# 2025 — verified against the published NSE/BSE trading holiday circular
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2025 = {
    date(2025, 2, 26): "Mahashivratri",
    date(2025, 3, 14): "Holi",
    date(2025, 3, 31): "Id-Ul-Fitr (Ramzan Id)",
    date(2025, 4, 10): "Shri Mahavir Jayanti",
    date(2025, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2025, 4, 18): "Good Friday",
    date(2025, 5, 1): "Maharashtra Day",
    date(2025, 8, 15): "Independence Day",
    date(2025, 8, 27): "Ganesh Chaturthi",
    date(2025, 10, 2): "Mahatma Gandhi Jayanti / Dussehra",
    date(2025, 10, 21): "Diwali Laxmi Pujan",
    date(2025, 10, 22): "Diwali-Balipratipada",
    date(2025, 11, 5): "Prakash Gurpurb Sri Guru Nanak Dev",
    date(2025, 12, 25): "Christmas",
}

# ---------------------------------------------------------------------------
# 2026 — fixed-date holidays only (high confidence, computable without an
# official circular). DO NOT treat this as the complete 2026 calendar.
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2026 = {
    date(2026, 1, 26): "Republic Day",
    date(2026, 4, 3): "Good Friday",
    date(2026, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2026, 5, 1): "Maharashtra Day",
    date(2026, 10, 2): "Mahatma Gandhi Jayanti",
    date(2026, 12, 25): "Christmas",
    # 2026-08-15 (Independence Day) falls on a Saturday — already a non-trading
    # day, so it is omitted here for consistency with how NSE publishes the
    # circular (matches the 2025 list, which omits Republic Day for the same
    # reason — 2025-01-26 fell on a Sunday).
}

# TODO(SPEC-SCHED-008): Add lunar/festival holidays for 2026 once the official
# NSE circular is published (expected ~December 2025). Known holiday names to
# expect, in approximate calendar order: Mahashivratri, Holi, Id-Ul-Fitr,
# Ram Navami / Shri Mahavir Jayanti, Ganesh Chaturthi, Dussehra,
# Diwali Laxmi Pujan, Diwali-Balipratipada, Guru Nanak Jayanti.
NSE_HOLIDAYS_2026_PENDING: dict = {}

ALL_NSE_HOLIDAYS = {
    **NSE_HOLIDAYS_2025,
    **NSE_HOLIDAYS_2026,
    **NSE_HOLIDAYS_2026_PENDING,
}


def is_nse_holiday(check_date: date) -> bool:
    """
    Return True if check_date is a declared NSE trading holiday.

    Parameters
    ----------
    check_date : date
        Calendar date to check.

    Returns
    -------
    bool

    Spec References
    ----------------
    SPEC-SCHED-008: Holidays excluded from gap detection — no backfill attempted.

    PIT Assumptions
    ----------------
    None — this is a static calendar lookup, not a data join.

    Raises
    ------
    None
    """
    return check_date in ALL_NSE_HOLIDAYS

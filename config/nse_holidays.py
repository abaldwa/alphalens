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

RCA note (2026-07-05): this file previously only covered 2025 and a partial
2026 (fixed-date holidays only). Every year 2005-2024 was completely absent,
so is_nse_holiday() returned False for every real historical holiday in that
range. That caused two confirmed, connected data-integrity bugs:
  1. ingestion/scheduler/gap_detector.py's is_trading_day() treated real
     holidays as ordinary missed trading days, so historical catch-up runs
     called ingestion/scrapers/bhavcopy.download_bhavcopy() for those dates.
     NSE's archive silently returned the last available (stale) bhavcopy
     file instead of erroring, and download_bhavcopy() never validated the
     fetched CSV's own trade date (DATE1 column) against the requested
     date — so equities ended up with exact-duplicate OHLCV/delivery rows
     on real holidays (confirmed on 2024-08-15, 2024-10-02, 2024-11-20,
     2024-12-25 etc. across many tickers).
  2. features/technical.py's beta_63d/alpha_21d use a rolling(63,
     min_periods=63) window against the NIFTYBEES/NIF100BEES/MONIFTY500
     benchmark ETFs, which correctly have NO row on real holidays. A single
     missing benchmark date nulls out up to 63 consecutive days of output —
     this incomplete holiday calendar meant real holidays kept surfacing as
     "gaps" against the benchmark's correct calendar.

Sources merged into this file:
  - 2005-2020: https://github.com/jugaad-py/master-data/blob/master/holidays/holidays.csv
    (dates only; no holiday names recorded in that source)
  - 2021-2024: indiainfoline.com/nse-holidays-{year} pages
  - 2025: unchanged from the prior version of this file (already verified
    against the published NSE/BSE circular)
  - 2026: NSE's own exchange-communication-holidays page (full list,
    superseding the old fixed-date-only placeholder)
"""

from datetime import date

# ---------------------------------------------------------------------------
# 2005-2020 — dates only (source: jugaad-py historical archive; no holiday
# names recorded in that source)
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2005_2020 = {
    date(2005, 1, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 3, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 7, 28): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 9, 7): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 10, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 11, 3): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 11, 4): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2005, 11, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 1, 11): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 2, 9): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 3, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 4, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 4, 11): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 10, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 10, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2006, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 1, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 1, 30): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 2, 16): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 3, 27): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 4, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 5, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 12, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2007, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 3, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 3, 20): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 3, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 4, 18): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 5, 19): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 9, 3): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 10, 9): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 10, 30): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 11, 13): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 11, 27): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 12, 9): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2008, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 1, 8): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 2, 23): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 3, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 3, 11): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 4, 3): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 4, 7): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 4, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 4, 30): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 9, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 9, 28): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 10, 13): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 10, 19): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 11, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2009, 12, 28): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 1, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 2, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 3, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 3, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 4, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 9, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 11, 17): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2010, 12, 17): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 3, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 4, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 4, 22): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 8, 31): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 9, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 10, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 10, 27): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 11, 7): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 11, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2011, 12, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 2, 20): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 3, 8): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 4, 5): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 4, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 8, 20): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 9, 19): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 10, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 11, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 11, 28): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2012, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 3, 27): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 3, 29): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 4, 19): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 4, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 8, 9): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 9, 9): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 10, 16): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 11, 4): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 11, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2013, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 2, 27): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 3, 17): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 4, 8): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 4, 18): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 4, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 7, 29): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 8, 29): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 10, 3): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 10, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 10, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 10, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 11, 4): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 11, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2014, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 2, 17): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 3, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 4, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 4, 3): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 9, 17): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 9, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 10, 22): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 11, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 11, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2015, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 3, 7): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 3, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 3, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 4, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 4, 19): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 7, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 9, 5): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 9, 13): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 10, 11): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 10, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 10, 31): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2016, 11, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 2, 24): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 3, 13): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 4, 4): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 6, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 8, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 10, 20): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2017, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 2, 13): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 3, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 3, 29): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 3, 30): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 8, 22): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 9, 13): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 9, 20): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 10, 18): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 11, 8): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 11, 23): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2018, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 1, 26): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 3, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 3, 4): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 3, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 4, 17): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 4, 19): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 4, 29): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 6, 5): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 8, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 8, 15): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 9, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 9, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 10, 8): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 10, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 10, 28): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 11, 12): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2019, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 2, 21): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 3, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 4, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 4, 6): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 4, 10): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 4, 14): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 5, 1): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    # 2020-07-31 removed 2026-07-05: cross-checked against ohlcv_adjusted and
    # NSE traded normally that day (real, distinct OHLCV per ticker, not a
    # holiday) — the jugaad-py source list is wrong for this specific date.
    date(2020, 10, 2): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 11, 16): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 11, 30): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
    date(2020, 12, 25): "NSE Holiday (source: jugaad-py historical archive, name not recorded)",
}

# ---------------------------------------------------------------------------
# 2021 — indiainfoline.com/nse-holidays-2021
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2021 = {
    date(2021, 1, 26): "Republic Day",
    date(2021, 3, 11): "Mahashivratri",
    date(2021, 3, 29): "Holi",
    date(2021, 4, 2): "Good Friday",
    date(2021, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2021, 4, 21): "Ram Navami",
    date(2021, 5, 13): "Id-Ul-Fitr (Ramzan Id)",
    date(2021, 7, 21): "Bakri Id",
    date(2021, 8, 19): "Muharram",
    date(2021, 9, 10): "Ganesh Chaturthi",
    date(2021, 10, 15): "Dussehra",
    date(2021, 11, 4): "Diwali Laxmi Pujan",
    date(2021, 11, 5): "Diwali Balipratipada",
    date(2021, 11, 19): "Gurunanak Jayanti",
}

# ---------------------------------------------------------------------------
# 2022 — indiainfoline.com/nse-holidays-2022
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2022 = {
    date(2022, 1, 26): "Republic Day",
    date(2022, 3, 1): "Mahashivratri",
    date(2022, 3, 18): "Holi",
    date(2022, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2022, 4, 15): "Good Friday",
    date(2022, 5, 3): "Id-Ul-Fitr (Ramzan Id)",
    date(2022, 8, 9): "Muharram",
    date(2022, 8, 15): "Independence Day",
    date(2022, 8, 31): "Ganesh Chaturthi",
    date(2022, 10, 5): "Dussehra",
    date(2022, 10, 24): "Diwali Laxmi Pujan",
    date(2022, 10, 26): "Diwali Balipratipada",
    date(2022, 11, 8): "Gurunanak Jayanti",
}

# ---------------------------------------------------------------------------
# 2023 — indiainfoline.com/nse-holidays-2023
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2023 = {
    date(2023, 1, 26): "Republic Day",
    date(2023, 3, 7): "Holi",
    date(2023, 3, 30): "Ram Navami",
    date(2023, 4, 4): "Mahavir Jayanti",
    date(2023, 4, 7): "Good Friday",
    date(2023, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2023, 5, 1): "Maharashtra Day",
    date(2023, 6, 29): "Bakri Id",
    date(2023, 8, 15): "Independence Day",
    date(2023, 9, 19): "Ganesh Chaturthi",
    date(2023, 10, 2): "Mahatma Gandhi Jayanti",
    date(2023, 10, 24): "Dussehra",
    date(2023, 11, 14): "Diwali Balipratipada",
    date(2023, 11, 27): "Gurunanak Jayanti",
    date(2023, 12, 25): "Christmas",
}

# ---------------------------------------------------------------------------
# 2024 — indiainfoline.com/nse-holidays-2024
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2024 = {
    date(2024, 1, 22): "Public Holiday",
    date(2024, 1, 26): "Republic Day",
    date(2024, 3, 8): "Mahashivratri",
    date(2024, 3, 25): "Holi",
    date(2024, 3, 29): "Good Friday",
    date(2024, 4, 11): "Id-Ul-Fitr (Ramadan Eid)",
    date(2024, 4, 17): "Shri Ram Navmi",
    date(2024, 5, 1): "Maharashtra Day",
    date(2024, 5, 20): "General Elections (Lok Sabha)",
    date(2024, 6, 17): "Bakri Id",
    date(2024, 7, 17): "Moharram",
    date(2024, 8, 15): "Independence Day",
    date(2024, 10, 2): "Mahatma Gandhi Jayanti",
    date(2024, 11, 1): "Diwali Laxmi Pujan",
    date(2024, 11, 15): "Gurunanak Jayanti",
    date(2024, 11, 20): "Assembly Elections - Maharashtra",
    date(2024, 12, 25): "Christmas",
}

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
# 2026 — full official list, nseindia.com/resources/exchange-communication-holidays
# (supersedes the old fixed-date-only placeholder + pending-lunar-holidays TODO)
# ---------------------------------------------------------------------------
NSE_HOLIDAYS_2026 = {
    date(2026, 1, 15): "Municipal Corporation Election - Maharashtra",
    date(2026, 1, 26): "Republic Day",
    date(2026, 3, 3): "Holi",
    date(2026, 3, 26): "Shri Ram Navami",
    date(2026, 3, 31): "Shri Mahavir Jayanti",
    date(2026, 4, 3): "Good Friday",
    date(2026, 4, 14): "Dr. Baba Saheb Ambedkar Jayanti",
    date(2026, 5, 1): "Maharashtra Day",
    date(2026, 5, 28): "Bakri Id",
    date(2026, 6, 26): "Muharram",
    date(2026, 9, 14): "Ganesh Chaturthi",
    date(2026, 10, 2): "Mahatma Gandhi Jayanti",
    date(2026, 10, 20): "Dussehra",
    date(2026, 11, 10): "Diwali-Balipratipada",
    date(2026, 11, 24): "Prakash Gurpurb Sri Guru Nanak Dev",
    date(2026, 12, 25): "Christmas",
}

ALL_NSE_HOLIDAYS = {
    **NSE_HOLIDAYS_2005_2020,
    **NSE_HOLIDAYS_2021,
    **NSE_HOLIDAYS_2022,
    **NSE_HOLIDAYS_2023,
    **NSE_HOLIDAYS_2024,
    **NSE_HOLIDAYS_2025,
    **NSE_HOLIDAYS_2026,
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

"""
features/corporate_action_features.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-FEAT-002, SPEC-PIPE-002, SPEC-PIPE-003, SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder (wired in P2.3)

Computes the 10 corporate-action features named in the P2.2 build prompt
(matches 01_features.md's "Corporate Action Features (10)" list exactly —
no name divergence this time, unlike P2.1's fundamental/governance lists).

[AS BUILT] Real, honest coverage split — not every feature is computable
from data this codebase actually ingests today:

REAL today (5, updated 2026-07-07): days_to_record_date,
corp_action_anticipation_return, ipo_lockin_expiry_proximity,
ipo_listing_age_months, post_earnings_drift_signal. `corporate_actions`
now has real SPLIT/BONUS (894 rows) and real BUYBACK rows (131 rows,
ingested via NSE's corporates-corporateActions feed — see
ingestion/scrapers/corporate_actions.py) as of this session. The PIT
filter in `_pit_filter_actions` was fixed (2026-07-07) to stop requiring
`announcement_date` (which NSE's endpoint never populates directly —
`caBroadcastDate` confirmed live, 2006-2026, to always be null) and
instead prefer a derived `announcement_date` (`record_date -
CORP_ACTION_NOTICE_DAYS`, a SEBI LODR Reg 42(2)-based conservative
lower bound — see ingestion/scrapers/corporate_actions.py's docstring),
falling back further to `record_date` then `ex_date` when even
record_date is unknown — see that function's docstring for the full
reasoning. This was a genuine PIT-filter bug, not a missing-data gap: it
silently zeroed out every corporate-action row for every feature in
this module before the fix, contradicting this docstring's previous
(stale) claim that only days_to_record_date/corp_action_anticipation_
return were real.

`buyback_price_spread` uses `ratio` as the offer price — NSE's BUYBACK
purpose strings this codebase currently sees ("Buy Back", no embedded
price) do not carry a parseable offer price, so `ratio` is honestly 0.0
("unknown") for all 131 BUYBACK rows today. This function now guards
against treating that 0.0 as a real price (`if close and buyback_price`)
— it used to silently compute a fabricated ~-100% spread for every
buyback, which would have violated this codebase's no-synthetic-data
policy. `buyback_price_spread` therefore moves to the NaN-by-design
bucket below until the offer-price extraction itself exists — see
`_parse_purpose`'s BUYBACK branch in ingestion/scrapers/
corporate_actions.py — a real, separate gap this fix does not close.

Genuinely NaN-by-design today (5): buyback_price_spread,
buyback_acceptance_estimated, index_inclusion_days, qip_dilution_impact,
dividend_yield_vs_fd_rate — these need corporate-action *types*
(INDEX_INCLUSION, QIP with a real dilution ratio), a parseable BUYBACK
offer price, and a DIVIDEND-yield/FD-rate data source this codebase does
not ingest at all (no INDEX_INCLUSION rows exist; QIP rows exist in the
action_type enum but 0 have ever been observed from NSE's feed in this
dataset; buyback_acceptance_estimated needs buyback size + free float,
not present in the corporate_actions schema). The computation logic here
is correct and ready the moment those rows/values exist — same "honest
NaN, not fabricated" precedent as P2.1's screener.py-sourced gaps
(gross_profit, capex, current_assets, ...).

PIT Assumptions
----------------
`corporate_actions` rows are read via DataStoreClient.get_corporate_actions
(PITRule.NONE at the API layer — SPEC-DS-002) and filtered here by
`announcement_date <= as_of` before use, exactly mirroring SPEC-PIPE-003's
fundamentals/shareholding PIT pattern: a corporate action whose
announcement_date is in the future relative to as_of was not yet public
knowledge and must never influence a feature computed as of that date.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import (
    ASSUMED_FD_RATE,
    CORP_ACTION_ANTICIPATION_WINDOW_DAYS,
    IPO_LOCKIN_DAYS,
    POST_EARNINGS_DRIFT_WINDOW_DAYS,
)
from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

CORPORATE_ACTION_FEATURES: List[str] = [
    "days_to_record_date",
    "corp_action_anticipation_return",
    "buyback_price_spread",
    "buyback_acceptance_estimated",
    "index_inclusion_days",
    "ipo_lockin_expiry_proximity",
    "ipo_listing_age_months",
    "post_earnings_drift_signal",
    "dividend_yield_vs_fd_rate",
    "qip_dilution_impact",
]


def _pit_filter_actions(actions: List[Dict[str, Any]], as_of: datetime) -> pd.DataFrame:
    """
    SPEC-PIPE-003: keep only actions that were publicly knowable as of `as_of`.

    [AS BUILT, bug fix 2026-07-07] `announcement_date` is set to None for
    every row ingestion/scrapers/corporate_actions.py writes (confirmed
    live against the real DuckDB: `select count(announcement_date) from
    corporate_actions` -> 0 across all 7669 rows / all 7 action_type
    values, not just BUYBACK) because NSE's corporates-corporateActions
    endpoint genuinely does not expose a separate announcement date field
    (see that module's docstring). The original filter required
    `announcement_date.notna()`, which is therefore *never* true — this
    silently zeroed out every row for every ticker regardless of as_of,
    making days_to_record_date/corp_action_anticipation_return/
    buyback_price_spread/index_inclusion_days/qip_dilution_impact always
    NaN even though the module docstring claimed the first two were
    "REAL today". That claim was stale/incorrect.

    PIT-safe fix without fabricating an announcement date: fall back to
    `record_date` (when present — NSE sets this whenever a record date
    applies, and by construction a record date is only published once the
    action is public) and finally to `ex_date` (always present; NSE only
    lists a corporate action in this feed once it has already been
    announced, so ex_date is a real, conservative upper bound on public
    knowledge — never an earlier, more-favourable date than the truth).
    This never look-aheads: the effective "known as of" date used is
    always >= the true (unrecorded) announcement date.
    """
    if not actions:
        return pd.DataFrame(columns=["ticker", "ex_date", "action_type", "ratio", "announcement_date", "record_date", "known_as_of"])
    df = pd.DataFrame(actions)
    df["ex_date"] = pd.to_datetime(df["ex_date"])
    df["announcement_date"] = pd.to_datetime(df["announcement_date"])
    df["record_date"] = pd.to_datetime(df["record_date"])
    df["known_as_of"] = df["announcement_date"].fillna(df["record_date"]).fillna(df["ex_date"])
    return df[df["known_as_of"].notna() & (df["known_as_of"] <= as_of)].sort_values("ex_date")


def _close_on_or_before(rows: List[Dict[str, Any]], target: datetime) -> Optional[float]:
    eligible = [r for r in rows if pd.to_datetime(r["date"]) <= target]
    if not eligible:
        return None
    return sorted(eligible, key=lambda r: r["date"])[-1]["close"]


def compute_corporate_action_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    listing_date: Optional[datetime] = None,
    pre_loaded_actions=None,
    pre_loaded_fundamentals=None,
    ticker_ohlcv: "Optional[pd.DataFrame]" = None,
) -> Dict[str, Any]:
    """
    Compute all 10 corporate-action features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
        SPEC-DS-002: all corporate_actions/OHLCV/fundamentals access via the API.
    ticker : str
    as_of : datetime
        PIT reference date.
    listing_date : datetime, optional
        From config.universe/stock_master — needed for the two IPO
        features. None if unknown (both features NaN).

    Returns
    -------
    dict
        feature_name -> value for all 10 CORPORATE_ACTION_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-002, SPEC-PIPE-003 (CRITICAL).

    PIT Assumptions
    ----------------
    See module docstring — `announcement_date <= as_of` filtering applied
    to every `corporate_actions` row before use.

    Raises
    ------
    None — missing data degrades to NaN, not an exception.
    """
    raw_actions = pre_loaded_actions if pre_loaded_actions is not None else client.get_corporate_actions(ticker)
    actions = _pit_filter_actions(raw_actions, as_of)

    future_record = actions[actions["record_date"].notna() & (actions["record_date"] > as_of)]
    days_to_record_date = (
        (future_record.iloc[0]["record_date"] - as_of).days if len(future_record) else np.nan
    )

    past_actions = actions[actions["ex_date"] <= as_of]
    corp_action_anticipation_return = np.nan
    if len(past_actions):
        nearest_ex_date = past_actions.iloc[-1]["ex_date"]
        window_start = nearest_ex_date - timedelta(days=CORP_ACTION_ANTICIPATION_WINDOW_DAYS)
        if window_start <= as_of:
            window_end = min(nearest_ex_date, as_of)
            if ticker_ohlcv is not None and not ticker_ohlcv.empty:
                w = ticker_ohlcv[
                    (ticker_ohlcv["date"] >= pd.Timestamp(window_start))
                    & (ticker_ohlcv["date"] <= pd.Timestamp(window_end))
                ].sort_values("date")
                if len(w) >= 2:
                    corp_action_anticipation_return = (
                        float(w.iloc[-1]["close"]) / float(w.iloc[0]["close"])
                    ) - 1.0
            else:
                price_rows = client.get_ohlcv(ticker, from_date=window_start, to_date=window_end)
                if len(price_rows) >= 2:
                    ordered = sorted(price_rows, key=lambda r: r["date"])
                    corp_action_anticipation_return = (ordered[-1]["close"] / ordered[0]["close"]) - 1.0

    # BUYBACK rows ARE ingested (131 real rows in corporate_actions), but
    # NSE's "purpose" text for them (e.g. "Buy Back-Tender Offer",
    # "Buyback") never states the tender price — confirmed live across
    # every real BUYBACK row — so _parse_purpose() correctly returns
    # ratio=0.0 for "price unknown," not "price is zero". QIP/
    # INDEX_INCLUSION action types are genuinely never written by any
    # ingestion module in this codebase today (module docstring).
    buyback_rows = actions[actions["action_type"] == "BUYBACK"]
    buyback_price_spread = np.nan
    if len(buyback_rows):
        if ticker_ohlcv is not None and not ticker_ohlcv.empty:
            w = ticker_ohlcv[ticker_ohlcv["date"] <= pd.Timestamp(as_of)].sort_values("date")
            close = float(w.iloc[-1]["close"]) if not w.empty else None
        else:
            price_rows = client.get_ohlcv(ticker, from_date=as_of - timedelta(days=14), to_date=as_of)
            close = _close_on_or_before(price_rows, as_of)
        buyback_price = buyback_rows.iloc[-1]["ratio"]  # convention: ratio holds the offer price for BUYBACK rows
        # buyback_price == 0.0 means "unknown" (see comment above), never a
        # real tender price — computing a spread against it would silently
        # fabricate a -100% spread for every buyback. Stay honestly NaN.
        if close and buyback_price:
            buyback_price_spread = (buyback_price - close) / close
    buyback_acceptance_estimated = np.nan  # needs buyback size + free float, not in corporate_actions schema

    index_inclusion_rows = actions[actions["action_type"] == "INDEX_INCLUSION"]
    index_inclusion_days = (
        (as_of - index_inclusion_rows.iloc[-1]["ex_date"]).days if len(index_inclusion_rows) else np.nan
    )

    ipo_lockin_expiry_proximity = np.nan
    ipo_listing_age_months = np.nan
    if listing_date is not None:
        lockin_expiry = listing_date + timedelta(days=IPO_LOCKIN_DAYS)
        ipo_lockin_expiry_proximity = (lockin_expiry - as_of).days
        ipo_listing_age_months = (as_of - listing_date).days / 30.44

    post_earnings_drift_signal = np.nan
    fundamentals_history = (
        pre_loaded_fundamentals if pre_loaded_fundamentals is not None
        else client.get_fundamentals_history(ticker, as_of, lookback_years=1)
    )
    if fundamentals_history:
        latest = sorted(fundamentals_history, key=lambda r: r["announcement_date"])[-1]
        announcement_date = pd.to_datetime(latest["announcement_date"])
        drift_window_end = min(announcement_date + timedelta(days=POST_EARNINGS_DRIFT_WINDOW_DAYS), as_of)
        if drift_window_end > announcement_date:
            if ticker_ohlcv is not None and not ticker_ohlcv.empty:
                w = ticker_ohlcv[
                    (ticker_ohlcv["date"] >= announcement_date)
                    & (ticker_ohlcv["date"] <= pd.Timestamp(drift_window_end))
                ].sort_values("date")
                if len(w) >= 2:
                    post_earnings_drift_signal = (
                        float(w.iloc[-1]["close"]) / float(w.iloc[0]["close"])
                    ) - 1.0
            else:
                price_rows = client.get_ohlcv(ticker, from_date=announcement_date, to_date=drift_window_end)
                if len(price_rows) >= 2:
                    ordered = sorted(price_rows, key=lambda r: r["date"])
                    post_earnings_drift_signal = (ordered[-1]["close"] / ordered[0]["close"]) - 1.0

    # No DIVIDEND rows ingested anywhere yet (module docstring) — NaN until
    # a real trailing-dividend source exists. ASSUMED_FD_RATE documented
    # for when it does.
    dividend_yield_vs_fd_rate = np.nan
    _ = ASSUMED_FD_RATE  # referenced for the future real implementation; see module docstring

    qip_rows = actions[actions["action_type"] == "QIP"]
    qip_dilution_impact = qip_rows.iloc[-1]["ratio"] if len(qip_rows) else np.nan

    return {
        "days_to_record_date": days_to_record_date,
        "corp_action_anticipation_return": corp_action_anticipation_return,
        "buyback_price_spread": buyback_price_spread,
        "buyback_acceptance_estimated": buyback_acceptance_estimated,
        "index_inclusion_days": index_inclusion_days,
        "ipo_lockin_expiry_proximity": ipo_lockin_expiry_proximity,
        "ipo_listing_age_months": ipo_listing_age_months,
        "post_earnings_drift_signal": post_earnings_drift_signal,
        "dividend_yield_vs_fd_rate": dividend_yield_vs_fd_rate,
        "qip_dilution_impact": qip_dilution_impact,
    }


def compute_corporate_action_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    listing_dates: Optional[Dict[str, datetime]] = None,
    data_cache=None,
    ohlcv_panel: "Optional[pd.DataFrame]" = None,
) -> pd.DataFrame:
    """
    Compute the 10-feature corporate-action panel for many tickers.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
    listing_dates : dict, optional
        ticker -> listing_date (e.g. from config.universe.load_universe()).

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + CORPORATE_ACTION_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-004: per-ticker loop is I/O orchestration, same exemption as
    features/fundamental.py's panel function.
    """
    listing_dates = listing_dates or {}
    records = []
    for ticker in tickers:
        try:
            pre_actions = data_cache.get_corp_actions(ticker) if data_cache is not None else None
            pre_fund = data_cache.get_fundamentals(ticker, as_of) if data_cache is not None else None
            t_ohlcv = (
                ohlcv_panel[ohlcv_panel["ticker"] == ticker] if ohlcv_panel is not None else None
            )
            feats = compute_corporate_action_features(
                client, ticker, as_of, listing_dates.get(ticker),
                pre_loaded_actions=pre_actions,
                pre_loaded_fundamentals=pre_fund,
                ticker_ohlcv=t_ohlcv,
            )
        except Exception as exc:
            logger.warning(f"corporate action features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in CORPORATE_ACTION_FEATURES}
        feats["ticker"] = ticker
        records.append(feats)

    panel = pd.DataFrame(records)
    return panel[["ticker"] + CORPORATE_ACTION_FEATURES]


def _vectorized_window_return(
    ohlcv_panel: Optional[pd.DataFrame],
    bounds: pd.DataFrame,
) -> pd.Series:
    """
    Shared helper for `corp_action_anticipation_return` and
    `post_earnings_drift_signal` — both are "(last_close / first_close) -
    1.0 over a per-ticker [window_start, window_end] window, NaN unless
    >= 2 OHLCV rows fall inside it" (same formula, same guard, only the
    bounds differ). `bounds` is indexed by ticker with `window_start`/
    `window_end` Timestamp columns (may be NaT — such tickers are simply
    excluded from the result, matching the sequential function's `if
    window_start <= as_of` / `if drift_window_end > announcement_date`
    guards).

    Returns
    -------
    pd.Series indexed by ticker — only tickers with a defined, >=2-row
    window are present (missing tickers must be treated as NaN by the caller).
    """
    if ohlcv_panel is None or ohlcv_panel.empty or bounds.empty:
        return pd.Series(dtype=float)
    b = bounds.dropna(subset=["window_start", "window_end"])
    if b.empty:
        return pd.Series(dtype=float)
    merged = ohlcv_panel.merge(b.reset_index(), on="ticker", how="inner")
    if merged.empty:
        return pd.Series(dtype=float)
    merged = merged[(merged["date"] >= merged["window_start"]) & (merged["date"] <= merged["window_end"])]
    if merged.empty:
        return pd.Series(dtype=float)
    merged = merged.sort_values(["ticker", "date"], kind="mergesort")
    g = merged.groupby("ticker", sort=False)
    counts = g.size()
    first_close = g["close"].first().astype(float)
    last_close = g["close"].last().astype(float)
    valid = counts >= 2
    ret = (last_close / first_close) - 1.0
    return ret[valid]


def compute_corporate_action_features_panel_vectorized(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    listing_dates: Optional[Dict[str, datetime]] = None,
    data_cache=None,
    ohlcv_panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Vectorized alternative to `compute_corporate_action_features_panel`:
    assembles every ticker's PIT-eligible corporate-action rows (and, for
    `post_earnings_drift_signal`, fundamentals rows) into shared
    DataFrames and computes all 10 features with pandas groupby
    operations instead of a per-ticker Python function call in a loop.

    Kept alongside (not replacing) `compute_corporate_action_features_panel`
    / `compute_corporate_action_features_panel_chunked` (commit 07d0122)
    as an alternative path — the sequential function remains the
    production baseline this is diffed against
    (tests/unit/test_corporate_action_features.py's parity tests).

    Preserves, unchanged from the sequential function:
      - `_pit_filter_actions`'s exact fallback chain
        (announcement_date -> record_date -> ex_date) — applied here to
        the whole multi-ticker frame at once, same filter logic, same
        `known_as_of <= as_of` gate.
      - The OHLCV window-return formula and its `len(window) >= 2` guard
        for both `corp_action_anticipation_return` and
        `post_earnings_drift_signal` (via `_vectorized_window_return`),
        including the live `client.get_ohlcv` fallback for tickers with
        no rows in `ohlcv_panel` (mirroring `ticker_ohlcv is not None and
        not ticker_ohlcv.empty` vs the sequential function's `else` branch).
      - `buyback_price_spread`'s `if close and buyback_price` guard against
        the ratio==0.0 ("unknown price") sentinel.

    Spec References
    ----------------
    SPEC-PIPE-002, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004.
    """
    listing_dates = listing_dates or {}

    action_rows: List[Dict[str, Any]] = []
    fundamentals_rows: List[Dict[str, Any]] = []
    for ticker in tickers:
        try:
            raw_actions = (
                data_cache.get_corp_actions(ticker) if data_cache is not None
                else client.get_corporate_actions(ticker)
            )
        except Exception as exc:
            logger.warning(f"corp_action vectorized: actions fetch failed for {ticker}: {exc}")
            raw_actions = []
        for r in raw_actions or []:
            r2 = dict(r)
            r2["ticker"] = ticker
            action_rows.append(r2)

        try:
            f_rows = (
                data_cache.get_fundamentals(ticker, as_of) if data_cache is not None
                else client.get_fundamentals_history(ticker, as_of, lookback_years=1)
            )
        except Exception as exc:
            logger.warning(f"corp_action vectorized: fundamentals fetch failed for {ticker}: {exc}")
            f_rows = []
        for r in f_rows or []:
            r2 = dict(r)
            r2["ticker"] = ticker
            fundamentals_rows.append(r2)

    as_of_ts = pd.Timestamp(as_of)
    result = pd.DataFrame({"ticker": list(tickers)}).set_index("ticker")
    for f in CORPORATE_ACTION_FEATURES:
        result[f] = np.nan

    # ── corporate_actions: PIT filter, once, across the whole panel ──────
    if action_rows:
        actions = pd.DataFrame(action_rows)
        actions["ex_date"] = pd.to_datetime(actions["ex_date"])
        actions["announcement_date"] = pd.to_datetime(actions["announcement_date"])
        actions["record_date"] = pd.to_datetime(actions["record_date"])
        actions["known_as_of"] = actions["announcement_date"].fillna(actions["record_date"]).fillna(actions["ex_date"])
        actions = actions[actions["known_as_of"].notna() & (actions["known_as_of"] <= as_of_ts)]
        actions = actions.sort_values(["ticker", "ex_date"], kind="mergesort")
    else:
        actions = pd.DataFrame(columns=["ticker", "ex_date", "action_type", "ratio", "announcement_date", "record_date", "known_as_of"])

    if not actions.empty:
        # days_to_record_date: nearest FUTURE record_date per ticker.
        future_record = actions[actions["record_date"].notna() & (actions["record_date"] > as_of_ts)]
        if not future_record.empty:
            future_record = future_record.sort_values(["ticker", "record_date"], kind="mergesort")
            first_future = future_record.groupby("ticker", sort=False).first()
            days = (first_future["record_date"] - as_of_ts).dt.days
            result.loc[days.index, "days_to_record_date"] = days

        # corp_action_anticipation_return: window ending at the nearest
        # PAST ex_date, starting CORP_ACTION_ANTICIPATION_WINDOW_DAYS
        # earlier, clipped so it never extends beyond as_of.
        past_actions = actions[actions["ex_date"] <= as_of_ts]
        if not past_actions.empty:
            nearest_ex = past_actions.groupby("ticker", sort=False)["ex_date"].last()
            window_start = nearest_ex - pd.Timedelta(days=CORP_ACTION_ANTICIPATION_WINDOW_DAYS)
            window_end = nearest_ex.clip(upper=as_of_ts)
            bounds = pd.DataFrame({"window_start": window_start, "window_end": window_end})
            bounds = bounds[bounds["window_start"] <= as_of_ts]
            ret, fallback_candidates = _window_return_with_fallback(
                client, ohlcv_panel, bounds, CORP_ACTION_ANTICIPATION_WINDOW_DAYS
            )
            if not ret.empty:
                result.loc[ret.index, "corp_action_anticipation_return"] = ret

        # buyback_price_spread: last close on/before as_of (global cutoff,
        # same for every ticker) vs. the latest BUYBACK row's ratio.
        buyback_rows = actions[actions["action_type"] == "BUYBACK"]
        if not buyback_rows.empty:
            latest_buyback = buyback_rows.groupby("ticker", sort=False).last()
            closes = _last_close_on_or_before(client, ohlcv_panel, list(latest_buyback.index), as_of_ts)
            for t, buyback_price in latest_buyback["ratio"].items():
                close = closes.get(t)
                if close and buyback_price:
                    result.loc[t, "buyback_price_spread"] = (buyback_price - close) / close

        index_rows = actions[actions["action_type"] == "INDEX_INCLUSION"]
        if not index_rows.empty:
            latest_index = index_rows.groupby("ticker", sort=False)["ex_date"].last()
            result.loc[latest_index.index, "index_inclusion_days"] = (as_of_ts - latest_index).dt.days

        qip_rows = actions[actions["action_type"] == "QIP"]
        if not qip_rows.empty:
            latest_qip = qip_rows.groupby("ticker", sort=False).last()
            result.loc[latest_qip.index, "qip_dilution_impact"] = latest_qip["ratio"]

    result["buyback_acceptance_estimated"] = np.nan  # NaN-by-design, see module docstring
    result["dividend_yield_vs_fd_rate"] = np.nan  # NaN-by-design, see module docstring
    _ = ASSUMED_FD_RATE  # referenced for the future real implementation; see module docstring

    # ── IPO features: pure date arithmetic, vectorizable via a Series map ──
    listing_series = pd.Series({t: listing_dates.get(t) for t in tickers if listing_dates.get(t) is not None})
    if not listing_series.empty:
        listing_ts = pd.to_datetime(listing_series)
        lockin_expiry = listing_ts + pd.Timedelta(days=IPO_LOCKIN_DAYS)
        result.loc[listing_ts.index, "ipo_lockin_expiry_proximity"] = (lockin_expiry - as_of_ts).dt.days
        result.loc[listing_ts.index, "ipo_listing_age_months"] = (as_of_ts - listing_ts).dt.days / 30.44

    # ── post_earnings_drift_signal ──────────────────────────────────────
    if fundamentals_rows:
        fdf = pd.DataFrame(fundamentals_rows)
        fdf["announcement_date"] = pd.to_datetime(fdf["announcement_date"])
        fdf = fdf.sort_values(["ticker", "announcement_date"], kind="mergesort")
        latest_fund = fdf.groupby("ticker", sort=False).last()
        window_start = latest_fund["announcement_date"]
        window_end = (window_start + pd.Timedelta(days=POST_EARNINGS_DRIFT_WINDOW_DAYS)).clip(upper=as_of_ts)
        bounds = pd.DataFrame({"window_start": window_start, "window_end": window_end})
        bounds = bounds[bounds["window_end"] > bounds["window_start"]]
        if not bounds.empty:
            ret, _ = _window_return_with_fallback(
                client, ohlcv_panel, bounds, POST_EARNINGS_DRIFT_WINDOW_DAYS
            )
            if not ret.empty:
                result.loc[ret.index, "post_earnings_drift_signal"] = ret

    result = result.reset_index()
    return result[["ticker"] + CORPORATE_ACTION_FEATURES]


def _window_return_with_fallback(
    client: DataStoreClient,
    ohlcv_panel: Optional[pd.DataFrame],
    bounds: pd.DataFrame,
    _lookback_days_unused: int,
) -> "tuple":
    """
    Computes `_vectorized_window_return` against the pre-sliced
    `ohlcv_panel`, then falls back to a live `client.get_ohlcv` call
    (per-ticker) for any ticker in `bounds` that has zero rows in
    `ohlcv_panel` — mirroring `ticker_ohlcv is not None and not
    ticker_ohlcv.empty` vs. the sequential function's `else` branch,
    exactly.
    """
    covered = set()
    if ohlcv_panel is not None and not ohlcv_panel.empty:
        covered = set(ohlcv_panel[ohlcv_panel["ticker"].isin(bounds.index)]["ticker"].unique())
    ret = _vectorized_window_return(ohlcv_panel, bounds)

    needs_fallback = [t for t in bounds.index if t not in covered]
    fallback_values = {}
    for t in needs_fallback:
        row = bounds.loc[t]
        price_rows = client.get_ohlcv(t, from_date=row["window_start"], to_date=row["window_end"])
        if len(price_rows) >= 2:
            ordered = sorted(price_rows, key=lambda r: r["date"])
            fallback_values[t] = (ordered[-1]["close"] / ordered[0]["close"]) - 1.0
    if fallback_values:
        ret = pd.concat([ret, pd.Series(fallback_values)])
    return ret, needs_fallback


def _last_close_on_or_before(
    client: DataStoreClient,
    ohlcv_panel: Optional[pd.DataFrame],
    tickers: List[str],
    as_of_ts: pd.Timestamp,
) -> Dict[str, Optional[float]]:
    """
    Last OHLCV close on or before `as_of_ts`, per ticker — vectorized
    against `ohlcv_panel` (same global cutoff for every ticker), with the
    same live `client.get_ohlcv` fallback used elsewhere in this module
    for tickers absent from `ohlcv_panel`.
    """
    closes: Dict[str, Optional[float]] = {}
    covered = set()
    if ohlcv_panel is not None and not ohlcv_panel.empty:
        covered = set(ohlcv_panel[ohlcv_panel["ticker"].isin(tickers)]["ticker"].unique())
        w = ohlcv_panel[ohlcv_panel["ticker"].isin(tickers) & (ohlcv_panel["date"] <= as_of_ts)]
        if not w.empty:
            w = w.sort_values(["ticker", "date"], kind="mergesort")
            last_close = w.groupby("ticker", sort=False)["close"].last()
            closes.update(last_close.to_dict())

    for t in tickers:
        if t in covered:
            continue
        price_rows = client.get_ohlcv(t, from_date=as_of_ts - timedelta(days=14), to_date=as_of_ts)
        closes[t] = _close_on_or_before(price_rows, as_of_ts)
    return closes

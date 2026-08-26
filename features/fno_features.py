"""
features/fno_features.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-FEAT-004, SPEC-PIPE-001, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder.py, systems/ml_signal_engine

16 F&O derivative features, computed only for F&O-eligible stocks
(SPEC-FEAT-004: "16 F&O features only for ~250 F&O-eligible stocks; NaN
for others — LightGBM handles via native missing-value support"). A
ticker is treated as F&O-eligible as of `as_of` if `fno_data` (the real,
persisted NSE F&O bhavcopy — see ingestion/scrapers/fno.py) has at least
one contract row for it within config.settings.FNO_ELIGIBILITY_LOOKBACK_DAYS
days before `as_of` — derived from real evidence already in the
DataStore, not a separately-maintained (and potentially stale) eligibility
list, since NSE revises the F&O-eligible stock set quarterly.

Data source: NSE's F&O bhavcopy (ingestion/scrapers/fno.py), the same
archive used for both historical backfill and same-day (post-close) data
— there is no separate live FYERS Option Chain scraper in this codebase;
the bhavcopy already carries real settle prices, open interest,
day-over-day OI change, and NSE's own reported underlying/spot price
(`UndrlygPric`) for every contract, which is sufficient to compute every
feature below without an additional live-quote source. This is a
deliberate scope choice, not a corner cut: the EOD bhavcopy is PIT-safe
(same-day knowable, like OHLCV) and is what every other ingestion module
in this project already treats as canonical for end-of-day analytics.

Feature list (the literal 16 names from this phase's build prompt — see
BuildLog.md "P2.3" for the documented divergence from 01_features.md's
differently-named 16-feature list, same "prompt text governs over older
reference docs" precedent already applied to features/fundamental.py):
pcr_oi, pcr_volume, iv_call, iv_put, iv_skew, atm_straddle_premium_pct,
oi_buildup_flag, oi_unwinding_flag, max_pain_level, max_pain_distance_pct,
option_chain_support, option_chain_resistance, synthetic_futures_spread,
rollover_cost, rollover_pcr, futures_basis_pct.

Implied volatility (iv_call, iv_put) is computed via Black-Scholes-Merton
inversion (Brent's method, scipy.optimize.brentq) against the ATM
option's real settle_price, using config.settings.INDIA_RISK_FREE_RATE
(a flat approximation — same documented-approximation precedent as
ASSUMED_FD_RATE/ASSUMED_TAX_RATE) and zero dividend yield. A premium that
doesn't bracket a solvable root in
[IV_SOLVER_MIN_VOL, IV_SOLVER_MAX_VOL] (e.g. a stale/zero-volume quote)
returns NaN rather than a clamped or fabricated value.

`rollover_pcr` is named for the literal prompt's feature name — despite
the "pcr" suffix, this is NOT a put-call ratio. It's defined here as the
far-month future's share of total (near + far) stock-futures open
interest (the standard "rollover %" metric), since the prompt names this
feature in the F&O list without a separate, more precisely-named
"rollover_pct" — see 01_features.md's own (differently-named) `rollover_pct`
for the same underlying concept.

PIT Assumptions
----------------
None — F&O bhavcopy is PITRule.NONE (same-day knowable, like OHLCV);
features/matrix_builder.py calls this for `as_of` = today only.
"""

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import norm

from config.settings import DUCKDB_PATH, FNO_ELIGIBILITY_LOOKBACK_DAYS, INDIA_RISK_FREE_RATE, IV_SOLVER_MAX_VOL, IV_SOLVER_MIN_VOL
from datastore.client import DataStoreClient

if TYPE_CHECKING:  # backfill_cache imports feature modules at runtime
    from features.backfill_cache import BackfillDataCache

logger = logging.getLogger(__name__)

# np.nan is typed as Any by this numpy's stubs; bind it once so the many
# "no data / degenerate window" early returns stay honestly typed as float.
_NAN: float = float(np.nan)



def load_ever_fno_eligible_tickers() -> Optional[Set[str]]:
    """
    Every ticker with at least one real STO/STF row anywhere in fno_data's
    history — no date filter, deliberately. Unlike config.universe's
    is_fno_eligible (relative to CURRENT_DATE, so it drifts as tickers gain/
    lose F&O activity), this is PIT-agnostic: safe to use as a pre-filter
    for compute_fno_features_panel's fno_eligible_tickers on ANY historical
    `as_of`, since a ticker absent from this set has never had F&O data at
    any point and can never resolve to anything but the all-NaN row
    get_fno_chain would return for it anyway — this only skips the
    guaranteed-empty API call, never changes what a given date returns.

    Returns
    -------
    Optional[Set[str]]
        The confirmed set of ever-F&O-eligible tickers, or None if it could
        not be determined (fno_data missing, or a transient error such as
        DuckDB lock contention against the live scheduler). Callers MUST
        treat None distinctly from an empty set: an empty set here would
        incorrectly claim "confirmed zero tickers are ever F&O-eligible"
        and (via compute_fno_features_panel's `is not None` pre-filter
        check) cause EVERY ticker to silently get an all-NaN F&O row —
        None correctly signals "unknown, fall back to the old per-ticker
        API behavior" instead. [BUG FIX 2026-07-28 model-review item 1]
    """
    try:
        from datastore.api.db import fno_db_path_for, get_duckdb_connection

        fno_path = fno_db_path_for(str(DUCKDB_PATH))
        if not fno_path.exists():
            logger.warning("fno_data not found at %s — no F&O eligibility pre-filter applied", fno_path)
            return None
        with get_duckdb_connection(fno_path, persist=False, read_only=True) as conn:
            df = conn.execute("SELECT DISTINCT ticker FROM fno_data WHERE instrument IN ('STO', 'STF')").df()
        return set(df["ticker"])
    except Exception as exc:
        logger.warning("Could not load ever-F&O-eligible tickers (%s) — no pre-filter applied", exc)
        return None

FNO_FEATURES: List[str] = [
    "pcr_oi",
    "pcr_volume",
    "iv_call",
    "iv_put",
    "iv_skew",
    "atm_straddle_premium_pct",
    "oi_buildup_flag",
    "oi_unwinding_flag",
    "max_pain_level",
    "max_pain_distance_pct",
    "option_chain_support",
    "option_chain_resistance",
    "synthetic_futures_spread",
    "rollover_cost",
    "rollover_pcr",
    "futures_basis_pct",
]


def _black_scholes_price(spot: float, strike: float, t_years: float, r: float, sigma: float, is_call: bool) -> float:
    """Black-Scholes-Merton premium, zero dividend yield."""
    if t_years <= 0 or sigma <= 0:
        return max(0.0, (spot - strike) if is_call else (strike - spot))
    d1 = (np.log(spot / strike) + (r + 0.5 * sigma**2) * t_years) / (sigma * np.sqrt(t_years))
    d2 = d1 - sigma * np.sqrt(t_years)
    if is_call:
        call: float = float(spot * norm.cdf(d1) - strike * np.exp(-r * t_years) * norm.cdf(d2))
        return call
    put: float = float(strike * np.exp(-r * t_years) * norm.cdf(-d2) - spot * norm.cdf(-d1))
    return put


def _implied_volatility(
    market_premium: float, spot: float, strike: float, t_years: float, r: float, is_call: bool
) -> float:
    """
    Invert Black-Scholes for sigma via Brent's method.

    Returns
    -------
    float
        Implied volatility, or NaN if the premium doesn't bracket a
        solvable root in [IV_SOLVER_MIN_VOL, IV_SOLVER_MAX_VOL] (e.g. a
        non-positive, stale, or arbitrage-violating quote) — never
        clamped or fabricated.
    """
    if pd.isna(market_premium) or market_premium <= 0 or t_years <= 0 or pd.isna(spot) or pd.isna(strike):
        return _NAN

    def objective(sigma: float) -> float:
        return _black_scholes_price(spot, strike, t_years, r, sigma, is_call) - market_premium

    try:
        lo, hi = objective(IV_SOLVER_MIN_VOL), objective(IV_SOLVER_MAX_VOL)
        if lo * hi > 0:
            return _NAN
        return float(brentq(objective, IV_SOLVER_MIN_VOL, IV_SOLVER_MAX_VOL, xtol=1e-6))
    except (ValueError, RuntimeError):
        return _NAN


def _max_pain(strikes: np.ndarray[Any, Any], call_oi: np.ndarray[Any, Any], put_oi: np.ndarray[Any, Any]) -> float:
    """
    Standard max-pain algorithm: the strike at which option WRITERS'
    total payout obligation (= buyers' aggregate intrinsic-value payout)
    is minimized.
    """
    best_strike, best_payout = np.nan, np.inf
    for k in strikes:
        call_payout: float = np.sum(np.maximum(0.0, k - strikes) * call_oi)
        put_payout: float = np.sum(np.maximum(0.0, strikes - k) * put_oi)
        total = call_payout + put_payout
        if total < best_payout:
            best_payout, best_strike = total, k
    return float(best_strike)


def _nearest_unexpired_expiry(expiries: pd.Series, as_of: datetime) -> Optional[pd.Timestamp]:
    future = expiries[expiries >= pd.Timestamp(as_of)]
    return future.min() if not future.empty else None


def compute_fno_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    pre_loaded_rows: Optional[List[Dict[str, Any]]] = None,
    pre_loaded_df: "Optional[pd.DataFrame]" = None,
) -> Dict[str, Any]:
    """
    Compute the 16-feature F&O panel for one ticker, as of `as_of`.

    Parameters
    ----------
    client : DataStoreClient
    ticker : str
    as_of : datetime
        Evaluation date — also the trade_date whose chain is used
        (F&O bhavcopy is same-day knowable).

    Returns
    -------
    dict
        Keys = FNO_FEATURES. All-NaN if the ticker has no F&O contracts
        in config.settings.FNO_ELIGIBILITY_LOOKBACK_DAYS days before
        `as_of` (SPEC-FEAT-004: not F&O-eligible).

    Spec References
    ----------------
    SPEC-FEAT-004, SPEC-SOLID-005 (OHLCV/F&O reached exclusively through
    DataStoreClient).

    PIT Assumptions
    ----------------
    None — PITRule.NONE, same-day knowable.

    Raises
    ------
    None — any per-feature computation failure (unsolvable IV, missing
    far-month future, etc.) degrades that feature to NaN, never raises.
    """
    empty = {f: np.nan for f in FNO_FEATURES}

    from_date = as_of - pd.Timedelta(days=FNO_ELIGIBILITY_LOOKBACK_DAYS)
    if pre_loaded_df is not None:
        if pre_loaded_df.empty:
            return empty
        chain = pre_loaded_df.copy()
    elif pre_loaded_rows is not None:
        if not pre_loaded_rows:
            return empty
        chain = pd.DataFrame(pre_loaded_rows)
    else:
        rows = client.get_fno_chain(ticker, from_date, as_of)
        if not rows:
            return empty
        chain = pd.DataFrame(rows)
    chain["trade_date"] = pd.to_datetime(chain["trade_date"])
    chain["expiry"] = pd.to_datetime(chain["expiry"])

    latest_date = chain["trade_date"].max()
    today = chain[chain["trade_date"] == latest_date]
    if today.empty:
        return empty

    spot = today["underlying_price"].dropna()
    if spot.empty:
        return empty
    spot = float(spot.iloc[0])

    futures = today[today["instrument"] == "STF"].sort_values("expiry")
    options = today[today["instrument"] == "STO"]

    out: Dict[str, Any] = dict(empty)

    # ===== Futures-derived features =====
    if not futures.empty:
        near = futures.iloc[0]
        if pd.notna(near["settle_price"]):
            out["futures_basis_pct"] = (near["settle_price"] - spot) / spot * 100.0
        out["oi_buildup_flag"] = float(near["oi_change"] > 0) if pd.notna(near["oi_change"]) else np.nan
        out["oi_unwinding_flag"] = float(near["oi_change"] < 0) if pd.notna(near["oi_change"]) else np.nan

        if len(futures) >= 2:
            far = futures.iloc[1]
            if pd.notna(near["settle_price"]) and pd.notna(far["settle_price"]):
                out["rollover_cost"] = float(far["settle_price"] - near["settle_price"])
            near_oi, far_oi = near["oi"] or 0, far["oi"] or 0
            if (near_oi + far_oi) > 0:
                out["rollover_pcr"] = float(far_oi / (near_oi + far_oi))

    # ===== Options-derived features (nearest unexpired expiry) =====
    if not options.empty:
        nearest_expiry = _nearest_unexpired_expiry(options["expiry"], as_of)
        chain_today = options[options["expiry"] == nearest_expiry] if nearest_expiry is not None else options.iloc[0:0]

        calls = chain_today[chain_today["option_type"] == "CE"].dropna(subset=["strike"])
        puts = chain_today[chain_today["option_type"] == "PE"].dropna(subset=["strike"])

        call_oi_sum = calls["oi"].fillna(0).sum()
        put_oi_sum = puts["oi"].fillna(0).sum()
        if call_oi_sum > 0:
            out["pcr_oi"] = float(put_oi_sum / call_oi_sum)

        call_vol_sum = calls["volume"].fillna(0).sum()
        put_vol_sum = puts["volume"].fillna(0).sum()
        if call_vol_sum > 0:
            out["pcr_volume"] = float(put_vol_sum / call_vol_sum)

        if not calls.empty and not puts.empty and nearest_expiry is not None:
            t_years = max((nearest_expiry - pd.Timestamp(as_of)).days, 0) / 365.0

            atm_call_idx = (calls["strike"] - spot).abs().idxmin()
            atm_put_idx = (puts["strike"] - spot).abs().idxmin()
            atm_call = calls.loc[atm_call_idx]
            atm_put = puts.loc[atm_put_idx]

            out["iv_call"] = _implied_volatility(
                atm_call["settle_price"], spot, atm_call["strike"], t_years, INDIA_RISK_FREE_RATE, is_call=True
            )
            out["iv_put"] = _implied_volatility(
                atm_put["settle_price"], spot, atm_put["strike"], t_years, INDIA_RISK_FREE_RATE, is_call=False
            )
            if pd.notna(out["iv_call"]) and pd.notna(out["iv_put"]):
                out["iv_skew"] = out["iv_put"] - out["iv_call"]

            if pd.notna(atm_call["settle_price"]) and pd.notna(atm_put["settle_price"]):
                out["atm_straddle_premium_pct"] = (
                    (atm_call["settle_price"] + atm_put["settle_price"]) / spot * 100.0
                )
                if not futures.empty and pd.notna(futures.iloc[0]["settle_price"]):
                    synthetic_future = atm_call["strike"] + (atm_call["settle_price"] - atm_put["settle_price"])
                    out["synthetic_futures_spread"] = float(synthetic_future - futures.iloc[0]["settle_price"])

            strikes_union = np.union1d(calls["strike"].to_numpy(), puts["strike"].to_numpy())
            if len(strikes_union) > 0:
                call_oi_by_strike = calls.groupby("strike")["oi"].sum().reindex(strikes_union, fill_value=0).to_numpy()
                put_oi_by_strike = puts.groupby("strike")["oi"].sum().reindex(strikes_union, fill_value=0).to_numpy()

                max_pain = _max_pain(strikes_union, call_oi_by_strike, put_oi_by_strike)
                out["max_pain_level"] = max_pain
                if pd.notna(max_pain):
                    out["max_pain_distance_pct"] = (spot - max_pain) / spot * 100.0

                if put_oi_by_strike.sum() > 0:
                    out["option_chain_support"] = float(strikes_union[np.argmax(put_oi_by_strike)])
                if call_oi_by_strike.sum() > 0:
                    out["option_chain_resistance"] = float(strikes_union[np.argmax(call_oi_by_strike)])

    return out


def compute_fno_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    data_cache: Optional["BackfillDataCache"] = None,
    fno_eligible_tickers: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """
    Compute the 16-feature F&O panel for many tickers.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
    fno_eligible_tickers : set of str, optional
        [2026-07-26 perf fix] Only ~180-200 of ~2,300 universe tickers are
        ever F&O-eligible (config.universe's is_fno_eligible), but this
        function used to call client.get_fno_chain for every ticker
        regardless — one live API round-trip per non-eligible ticker per
        date, ~2,100 wasted calls/day, the dominant cost of a full-universe
        backfill (confirmed: 2,317 of ~2,423 HTTP calls for one backtest
        day were this call). When supplied, non-eligible tickers skip the
        API call entirely and get the same all-NaN row SPEC-FEAT-004
        already specifies for them — behavior-preserving, pure perf fix.
        None (default) preserves the original call-everyone behavior for
        callers that don't have this set on hand.

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + FNO_FEATURES. All-NaN
        rows for non-F&O-eligible tickers (SPEC-FEAT-004).

    Spec References
    ----------------
    SPEC-PIPE-004: the per-ticker loop is I/O orchestration (one API call
    per ticker), same exemption as features/fundamental.py's panel function.
    """
    records = []
    for ticker in tickers:
        if fno_eligible_tickers is not None and ticker not in fno_eligible_tickers:
            feats: Dict[str, Any] = {f: np.nan for f in FNO_FEATURES}
            feats["ticker"] = ticker
            records.append(feats)
            continue
        try:
            pre_rows = (
                data_cache.get_fno(ticker, as_of - pd.Timedelta(days=FNO_ELIGIBILITY_LOOKBACK_DAYS), as_of)
                if data_cache is not None and hasattr(data_cache, "get_fno")
                else None
            )
            feats = compute_fno_features(client, ticker, as_of, pre_loaded_rows=pre_rows)
        except Exception as exc:
            logger.warning(f"F&O features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in FNO_FEATURES}
        feats["ticker"] = ticker
        records.append(feats)

    panel = pd.DataFrame(records)
    return panel[["ticker"] + FNO_FEATURES]

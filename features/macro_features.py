"""
features/macro_features.py

Phase: 1.1 (Core Feature Computation)
Specs: SPEC-FEAT-001, SPEC-PIPE-004, SPEC-PIPE-006, SPEC-DS-002
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine

Computes 14 market-wide macro/breadth features. Unlike features/technical.py,
these are not per-ticker — every stock in the universe shares the same
value on a given date, so this module returns exactly one row per date
(features/matrix_builder.py broadcasts it across the day's tickers).

Data availability (P1.2 update — see BuildLog.md): india_vix, usd_inr,
fii_net_5d, dii_net_5d, crude_oil_price, gold_price and yield_10yr are now
all backed by real ingestion sources (ingestion/scrapers/macro.py writes
INDIA_VIX, USD_INR, FII_NET_CR, DII_NET_CR, CRUDE_OIL, GOLD, YIELD_10YR,
YIELD_3M to DuckDB `macro_indicators`). yield_spread_10yr_2yr uses
YIELD_3M (a 3-month interbank/T-bill rate) as the short end of the curve —
a true daily India 2-year G-Sec series is not available from a free,
scrapeable source (RBI publishes PDF circulars, not JSON/CSV; CCIL's
G-Sec pages 403 non-browser clients) — see ingestion/scrapers/macro.py's
module docstring for the sources tried. This is a documented approximation,
not the literal 2yr spread. nifty_50_return_5d/21d and advance_decline_
ratio/market_breadth_21d remain dependent on benchmark/universe OHLCV
history being available for the requested date (see features/technical.py's
BENCHMARK_TICKERS note on the dev DB's thin ETF history). rl_regime_label
is an explicit Phase 1 stub (=0) per the build instructions; M-15 (PPO
meta-agent, Phase 4) is what eventually populates it.

SPEC-DS-002 permits direct DuckDB access "within ingestion and feature
layers" (unlike features/matrix_builder.py, which must go through the
DataStore API for OHLCV per SPEC-SOLID-005) — load_macro_indicators()
below reads `macro_indicators` directly for that reason.
"""

import logging
from datetime import date as date_type
from pathlib import Path
from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# np.nan is typed as Any by this numpy's stubs; bind it once so the many
# "no data / degenerate window" early returns stay honestly typed as float.
_NAN: float = float(np.nan)


MACRO_FEATURES = [
    "india_vix",
    "vix_5d_change",
    "usd_inr",
    "crude_oil_price",
    "gold_price",
    "nifty_50_return_5d",
    "nifty_50_return_21d",
    "advance_decline_ratio",
    "fii_net_5d",
    "dii_net_5d",
    "market_breadth_21d",
    "yield_10yr",
    "yield_spread_10yr_2yr",
    "rl_regime_label",
]

# Indicators with a live ingestion source (ingestion/scrapers/macro.py).
# Anything in MACRO_FEATURES not derivable from this set is NaN by design — see module docstring.
_INGESTED_INDICATORS = {
    "INDIA_VIX",
    "USD_INR",
    "FII_NET_CR",
    "DII_NET_CR",
    "CRUDE_OIL",
    "GOLD",
    "YIELD_10YR",
    "YIELD_3M",
}


def load_macro_indicators(
    as_of: Union[str, date_type],
    lookback_days: int = 30,
    db_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Read `macro_indicators` directly from DuckDB for [as_of - lookback_days, as_of].

    Parameters
    ----------
    as_of : str or date
        Reference date (inclusive).
    lookback_days : int
        Calendar-day window before `as_of` to pull (default 30 — enough
        for the 21-trading-day windows used below with holiday slack).
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    pd.DataFrame
        Columns: date, indicator, value.

    Spec References
    ----------------
    SPEC-DS-002: direct DuckDB access is permitted in the feature layer.

    Raises
    ------
    None — returns an empty DataFrame if the table/file doesn't exist yet.
    """
    from datastore.api.db import get_duckdb_connection
    from config.settings import DUCKDB_PATH

    if db_path is None:
        db_path = DUCKDB_PATH

    as_of_ts = pd.Timestamp(as_of)
    start = (as_of_ts - pd.Timedelta(days=lookback_days)).date()
    try:
        with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
            df = conn.execute(
                "SELECT date, indicator, value FROM macro_indicators WHERE date >= ? AND date <= ?",
                [start, as_of_ts.date()],
            ).fetchdf()
    except Exception as exc:
        logger.warning(f"Could not load macro_indicators: {exc}")
        return pd.DataFrame(columns=["date", "indicator", "value"])

    df["date"] = pd.to_datetime(df["date"])
    return df


def _indicator_series(macro_indicators: pd.DataFrame, name: str) -> pd.Series:
    sub = macro_indicators[macro_indicators["indicator"] == name].sort_values("date")
    return sub.set_index("date")["value"]


def _level_and_change(
    series: pd.Series, as_of: pd.Timestamp, lookback: int
) -> Tuple[float, float]:
    """Latest value as-of `as_of`, and its change vs `lookback` observations earlier."""
    s = series[series.index <= as_of]
    if s.empty:
        return _NAN, _NAN
    latest = float(s.iloc[-1])
    if len(s) <= lookback:
        return latest, np.nan
    return latest, latest - float(s.iloc[-1 - lookback])


def _trailing_sum(series: pd.Series, as_of: pd.Timestamp, window: int) -> float:
    s = series[series.index <= as_of]
    if s.empty:
        return _NAN
    return float(s.iloc[-window:].sum())


def _pct_return(close: pd.Series, window: int) -> float:
    if len(close) <= window:
        return _NAN
    base = close.iloc[-1 - window]
    if base == 0 or pd.isna(base):
        return _NAN
    return float(close.iloc[-1] / base - 1)


def _breadth_metrics(
    universe_ohlcv: pd.DataFrame, as_of: pd.Timestamp
) -> Tuple[float, float]:
    """
    Cross-sectional advance/decline ratio and % of universe above its 21d SMA.

    Vectorized via groupby/transform — the only iteration pandas performs
    internally is per-ticker group dispatch, not a Python loop (SPEC-PIPE-004).
    """
    panel = universe_ohlcv[universe_ohlcv["date"] <= as_of].sort_values(["ticker", "date"])
    today_mask = panel["date"] == as_of
    if not today_mask.any():
        return _NAN, _NAN

    prev_close = panel.groupby("ticker", sort=False)["close"].shift(1)
    chg = panel.loc[today_mask, "close"] - prev_close.loc[today_mask]
    advances = int((chg > 0).sum())
    declines = int((chg < 0).sum())
    adv_decl_ratio = advances / declines if declines > 0 else np.nan

    sma21 = panel.groupby("ticker", sort=False)["close"].transform(
        lambda s: s.rolling(21, min_periods=21).mean()
    )
    above_sma = panel.loc[today_mask, "close"] > sma21.loc[today_mask]
    breadth_pct = float(above_sma.mean() * 100) if above_sma.notna().any() else np.nan

    return adv_decl_ratio, breadth_pct


def compute_macro_features(
    date: Union[str, date_type],
    macro_indicators: pd.DataFrame,
    nifty50_ohlcv: pd.DataFrame,
    universe_ohlcv: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Compute the 14 macro features for a single date.

    Parameters
    ----------
    date : str or date
        Target date.
    macro_indicators : pd.DataFrame
        Columns: date, indicator, value — history through `date` (see
        load_macro_indicators). Indicators not in _INGESTED_INDICATORS
        simply produce NaN if absent.
    nifty50_ohlcv : pd.DataFrame
        Columns: date, close — Nifty 50 benchmark history through `date`
        (e.g. the NIFTYBEES proxy used by features/technical.py).
    universe_ohlcv : pd.DataFrame, optional
        Columns: date, ticker, close — full universe history through
        `date`, used for advance_decline_ratio / market_breadth_21d. If
        omitted, those two columns are NaN.

    Returns
    -------
    pd.DataFrame
        One row, columns: date + MACRO_FEATURES (14 cols), float64.

    Spec References
    ----------------
    SPEC-PIPE-006: india_vix/usd_inr/fii_net_5d/dii_net_5d sourced from the
    macro_indicators table written by ingestion/scrapers/macro.py.
    SPEC-FEAT-001: features needing more history than is available return NaN.

    PIT Assumptions
    ----------------
    All macro/breadth inputs are same-day-knowable (PITRule.NONE) — no PIT
    filtering needed; callers must still avoid passing rows dated > `date`.

    Raises
    ------
    None
    """
    as_of = pd.Timestamp(date)

    vix, vix_5d_change = _level_and_change(_indicator_series(macro_indicators, "INDIA_VIX"), as_of, 5)
    usd_inr, _ = _level_and_change(_indicator_series(macro_indicators, "USD_INR"), as_of, 5)
    fii_net_5d = _trailing_sum(_indicator_series(macro_indicators, "FII_NET_CR"), as_of, 5)
    dii_net_5d = _trailing_sum(_indicator_series(macro_indicators, "DII_NET_CR"), as_of, 5)
    crude_oil_price, _ = _level_and_change(_indicator_series(macro_indicators, "CRUDE_OIL"), as_of, 5)
    gold_price, _ = _level_and_change(_indicator_series(macro_indicators, "GOLD"), as_of, 5)
    yield_10yr, _ = _level_and_change(_indicator_series(macro_indicators, "YIELD_10YR"), as_of, 5)
    yield_3m, _ = _level_and_change(_indicator_series(macro_indicators, "YIELD_3M"), as_of, 5)
    yield_spread = yield_10yr - yield_3m if pd.notna(yield_10yr) and pd.notna(yield_3m) else np.nan

    nifty_close = nifty50_ohlcv.sort_values("date")
    nifty_close = nifty_close[nifty_close["date"] <= as_of]["close"]
    nifty_50_return_5d = _pct_return(nifty_close, 5)
    nifty_50_return_21d = _pct_return(nifty_close, 21)

    adv_decl_ratio, breadth_21d = (np.nan, np.nan)
    if universe_ohlcv is not None and not universe_ohlcv.empty:
        adv_decl_ratio, breadth_21d = _breadth_metrics(universe_ohlcv, as_of)

    row = {
        "date": as_of,
        "india_vix": vix,
        "vix_5d_change": vix_5d_change,
        "usd_inr": usd_inr,
        "crude_oil_price": crude_oil_price,
        "gold_price": gold_price,
        "nifty_50_return_5d": nifty_50_return_5d,
        "nifty_50_return_21d": nifty_50_return_21d,
        "advance_decline_ratio": adv_decl_ratio,
        "fii_net_5d": fii_net_5d,
        "dii_net_5d": dii_net_5d,
        "market_breadth_21d": breadth_21d,
        "yield_10yr": yield_10yr,
        # yield_3m (3-month interbank/T-bill) stands in for the 2yr leg — see module docstring.
        "yield_spread_10yr_2yr": yield_spread,
        "rl_regime_label": 0.0,  # Phase 1 stub; M-15 (PPO meta-agent, Phase 4) populates this later
    }
    out = pd.DataFrame([row])
    for col in MACRO_FEATURES:
        out[col] = out[col].astype(np.float64)
    return out

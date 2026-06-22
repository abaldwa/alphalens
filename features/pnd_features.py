"""
features/pnd_features.py

Phase: 1.3 (P&D Detection)
Specs: SPEC-MODEL-006, SPEC-FEAT-004, SPEC-PIPE-004
Owner: Platform / Features
Consumers: systems/ml_signal_engine/models/pnd/pnd_detector.py (M-06)

Computes 22 pump-and-dump (P&D) detection features from adjusted OHLCV +
delivery data. SPEC-MODEL-006 (CRITICAL): this is a pre-filter that runs
BEFORE any buy signal reaches the user — every feature here must be
computable purely from same-day-knowable OHLCV/delivery data (PITRule.NONE,
same as features/technical.py), never from anything that could leak
future information about whether a stock turns out to have been a P&D.

No circuit-limit reference data is ingested as of Phase 1 (NSE assigns
5%/10%/20% bands per stock, not uniformly) — "upper circuit" detection
here uses the classic OHLC signature of a circuit-locked day (no intraday
trading range: high == low, and the day closed up) rather than a fixed
percentage threshold, since that signature is circuit-band-agnostic.
`upper_circuit_proximity` and `circuit_filter_proximity_10d` instead
assume a 20% band off the prior close as a documented simplifying
proxy (most Nifty 500 / large-cap names trade in the 20% band) — see
BuildLog.md "P1.3" for the alternative considered and why it was rejected.

Microstructure features (bid_ask_spread_proxy, price_impact_ratio,
turnover_acceleration, operator_signature_score) are OHLCV-only proxies,
not true order-book/L2 microstructure (no tick/quote data is ingested) —
documented per-feature below. This mirrors SPEC-FEAT-004's "NaN where the
underlying data source doesn't exist; let the model handle it natively"
pattern, except here there's no NaN — the proxies are always computable,
just acknowledged as approximations of the named concept.

SPEC-PIPE-004: fully vectorized via pandas groupby/rolling, same pattern
as features/technical.py — no Python loop over individual stocks.
"""

import logging
from typing import List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]

# Proxy for NSE's per-stock circuit band (5%/10%/20%, not ingested as
# reference data) — see module docstring.
ASSUMED_CIRCUIT_BAND_PCT = 20.0

_VOLUME_ANOMALIES = [
    "vol_spike_ratio_3d",
    "vol_spike_ratio_5d",
    "vol_spike_vs_60d_avg",
    "volume_zscore_10d",
    "cumulative_vol_change_5d",
    "unusual_vol_days_count_10d",
]
_PRICE_ANOMALIES = [
    "consecutive_up_days",
    "consecutive_circuit_days",
    "price_acceleration_5d",
    "upper_circuit_proximity",
    "max_single_day_move_5d",
]
_DELIVERY_COLLAPSE = [
    "delivery_pct_3d_avg",
    "delivery_vs_4w_avg",
    "delivery_collapse_flag",
    "delivery_spike_then_collapse",
]
_MICROSTRUCTURE = [
    "bid_ask_spread_proxy",
    "price_impact_ratio",
    "turnover_acceleration",
    "operator_signature_score",
]
_CROSS_FEATURE = [
    "pnd_momentum_breakout",
    "circuit_filter_proximity_10d",
    "reversal_after_spike_flag",
]

PND_FEATURES: List[str] = _VOLUME_ANOMALIES + _PRICE_ANOMALIES + _DELIVERY_COLLAPSE + _MICROSTRUCTURE + _CROSS_FEATURE
assert len(PND_FEATURES) == 22, "PND_FEATURES catalog drifted from the spec'd 22"


def _grouped_rolling(df: pd.DataFrame, col: str, window: int, how: str, min_periods: int = None) -> pd.Series:
    grouped = df.groupby("ticker", sort=False)[col].rolling(window, min_periods=min_periods or window)
    return getattr(grouped, how)().reset_index(level=0, drop=True)


def _grouped_shift(df: pd.DataFrame, col: str, periods: int) -> pd.Series:
    return df.groupby("ticker", sort=False)[col].shift(periods)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator.to_numpy(dtype=np.float64) / denominator.to_numpy(dtype=np.float64)
    return pd.Series(result, index=numerator.index).replace([np.inf, -np.inf], np.nan)


def _consecutive_true_run(flags: pd.Series, ticker: pd.Series) -> pd.Series:
    """
    Length of the consecutive run of True values in `flags` ending at each
    row, restarting at 0 whenever flags is False or a new ticker starts.

    Vectorized via a "block id increments on every False row, cumsum of
    flag-as-int within each (ticker, block) group" trick — no per-row
    Python loop. (An earlier cumsum-on-breakpoint version of this
    function had an off-by-one: it grouped the breaking False row
    together with the True run that followed it, inflating every count
    by 1 — caught by tests/unit/test_pnd_features.py's "5 consecutive
    upper circuits" test, see BuildLog.md "P1.3".)
    """
    work = pd.DataFrame({"ticker": ticker.to_numpy(), "flag": flags.to_numpy().astype(int)})
    block_id = work.groupby("ticker")["flag"].transform(lambda s: (1 - s).cumsum())
    running = work.groupby([work["ticker"], block_id])["flag"].cumsum()
    return pd.Series(running.to_numpy() * work["flag"].to_numpy(), index=flags.index)


def _category_volume_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    vol_sma3 = _grouped_rolling(df, "volume", 3, "mean")
    vol_sma5 = _grouped_rolling(df, "volume", 5, "mean")
    vol_sma20 = _grouped_rolling(df, "volume", 20, "mean")
    vol_sma60 = _grouped_rolling(df, "volume", 60, "mean")
    vol_std10 = _grouped_rolling(df, "volume", 10, "std")
    vol_mean10 = _grouped_rolling(df, "volume", 10, "mean")

    out["vol_spike_ratio_3d"] = _safe_div(vol_sma3, vol_sma20)
    out["vol_spike_ratio_5d"] = _safe_div(vol_sma5, vol_sma20)
    out["vol_spike_vs_60d_avg"] = _safe_div(df["volume"], vol_sma60)
    out["volume_zscore_10d"] = _safe_div(df["volume"] - vol_mean10, vol_std10)

    vol_sum5 = _grouped_rolling(df, "volume", 5, "sum")
    vol_sum5_prior = _grouped_shift(df.assign(_vol_sum5=vol_sum5), "_vol_sum5", 5)
    out["cumulative_vol_change_5d"] = _safe_div(vol_sum5 - vol_sum5_prior, vol_sum5_prior)

    is_unusual = df["volume"] > 3 * vol_sma20
    work = df.assign(_unusual=is_unusual.astype(float))
    out["unusual_vol_days_count_10d"] = _grouped_rolling(work, "_unusual", 10, "sum")
    return out


def _category_price_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    prev_close = _grouped_shift(df, "close", 1)
    daily_ret = _safe_div(df["close"] - prev_close, prev_close)

    out["consecutive_up_days"] = _consecutive_true_run(daily_ret > 0, df["ticker"])

    # Circuit-locked day signature: no intraday range, closed up (see module docstring).
    is_upper_circuit = (df["high"] == df["low"]) & (df["close"] > prev_close)
    out["consecutive_circuit_days"] = _consecutive_true_run(is_upper_circuit, df["ticker"])

    ret_5d_now = _safe_div(df["close"] - _grouped_shift(df, "close", 5), _grouped_shift(df, "close", 5))
    close_5_back = _grouped_shift(df, "close", 5)
    close_10_back = _grouped_shift(df, "close", 10)
    ret_5d_prior = _safe_div(close_5_back - close_10_back, close_10_back)
    out["price_acceleration_5d"] = ret_5d_now - ret_5d_prior

    circuit_limit_proxy = prev_close * (1 + ASSUMED_CIRCUIT_BAND_PCT / 100.0)
    out["upper_circuit_proximity"] = _safe_div(circuit_limit_proxy - df["close"], circuit_limit_proxy)

    work = df.assign(_abs_ret=daily_ret.abs())
    out["max_single_day_move_5d"] = _grouped_rolling(work, "_abs_ret", 5, "max")
    return out


def _category_delivery_collapse(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    delivery_pct = df["delivery_pct"]
    avg_4w = _grouped_rolling(df, "delivery_pct", 20, "mean")
    out["delivery_pct_3d_avg"] = _grouped_rolling(df, "delivery_pct", 3, "mean")
    out["delivery_vs_4w_avg"] = _safe_div(delivery_pct, avg_4w)

    vol_sma20 = _grouped_rolling(df, "volume", 20, "mean")
    high_volume = df["volume"] > 2 * vol_sma20
    collapsed = delivery_pct < 0.5 * avg_4w
    out["delivery_collapse_flag"] = (high_volume & collapsed).astype(float)

    work = df.assign(_delivery_pct=delivery_pct, _avg_4w=avg_4w)
    spike_flag = work.assign(_spike=(delivery_pct > 1.5 * avg_4w).astype(float))
    had_spike_recently = _grouped_rolling(spike_flag, "_spike", 8, "max", min_periods=1)
    # Look at the spike flag from 2-10 days ago (a *prior* spike, not today's collapse day itself).
    had_spike_recently_shifted = _grouped_shift(work.assign(_hs=had_spike_recently), "_hs", 2)
    out["delivery_spike_then_collapse"] = (collapsed & (had_spike_recently_shifted > 0)).astype(float)
    return out


def _category_microstructure(df: pd.DataFrame) -> pd.DataFrame:
    """
    OHLCV-only proxies — no L2/order-book or tick data is ingested as of
    Phase 1, so these approximate the named microstructure concepts
    rather than measuring them directly. Documented per-feature.
    """
    out = pd.DataFrame(index=df.index)
    mid = (df["high"] + df["low"]) / 2
    # Range-based spread proxy (wider daily range ~ wider effective spread), not a quoted bid/ask.
    out["bid_ask_spread_proxy"] = _safe_div(df["high"] - df["low"], mid)

    prev_close = _grouped_shift(df, "close", 1)
    daily_ret = _safe_div(df["close"] - prev_close, prev_close)
    turnover = df["volume"] * df["close"]
    # Amihud-illiquidity-style: |return| per unit of INR turnover (scaled for readability).
    out["price_impact_ratio"] = _safe_div(daily_ret.abs() * 1e9, turnover)

    turnover_5d = _grouped_rolling(df.assign(_turnover=turnover), "_turnover", 5, "mean")
    turnover_20d = _grouped_rolling(df.assign(_turnover=turnover), "_turnover", 20, "mean")
    out["turnover_acceleration"] = _safe_div(turnover_5d, turnover_20d) - 1

    # Composite proxy: mean of 4 binary "operator-style" signals already computed elsewhere
    # in this module — recomputed locally (not cross-referenced) so this function stays
    # independent of category ordering.
    vol_sma20 = _grouped_rolling(df, "volume", 20, "mean")
    vol_sma5 = _grouped_rolling(df, "volume", 5, "mean")
    is_upper_circuit = (df["high"] == df["low"]) & (df["close"] > prev_close)
    circuit_recent = _grouped_rolling(df.assign(_uc=is_upper_circuit.astype(float)), "_uc", 10, "sum") > 0
    vol_spike = _safe_div(vol_sma5, vol_sma20) > 2
    avg_4w = _grouped_rolling(df, "delivery_pct", 20, "mean")
    delivery_low = df["delivery_pct"] < 0.5 * avg_4w
    accelerating = daily_ret > 0
    signals = pd.concat(
        [circuit_recent.astype(float), vol_spike.astype(float), delivery_low.astype(float), accelerating.astype(float)],
        axis=1,
    )
    out["operator_signature_score"] = signals.mean(axis=1)
    return out


def _category_cross_feature(df: pd.DataFrame, volume_out: pd.DataFrame, price_out: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    hi20_prior = _grouped_shift(df.assign(_hi20=_grouped_rolling(df, "high", 20, "max")), "_hi20", 1)
    breakout = df["close"] > hi20_prior
    out["pnd_momentum_breakout"] = (breakout & (volume_out["vol_spike_ratio_5d"] > 2)).astype(float)

    near_circuit = price_out["upper_circuit_proximity"] < 0.05
    work = df.assign(_near=near_circuit.astype(float))
    out["circuit_filter_proximity_10d"] = _grouped_rolling(work, "_near", 10, "sum")

    ret_3d = _safe_div(df["close"] - _grouped_shift(df, "close", 3), _grouped_shift(df, "close", 3))
    had_spike_3to10_ago = _grouped_shift(
        df.assign(_spike=(volume_out["vol_spike_ratio_5d"] > 2).astype(float)), "_spike", 3
    )
    had_spike_recently = (
        _grouped_rolling(df.assign(_hs=had_spike_3to10_ago.fillna(0)), "_hs", 7, "max", min_periods=1) > 0
    )
    out["reversal_after_spike_flag"] = (had_spike_recently & (ret_3d < 0)).astype(float)
    return out


def compute_pnd_features(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all 22 P&D detection features for a multi-ticker OHLCV panel.

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel with columns: date, ticker, open, high, low,
        close, volume, and optionally delivery_pct (NaN-filled if
        absent). One row per (ticker, date), full lookback history per
        ticker (>= 60 days to populate every feature; see
        vol_spike_vs_60d_avg).

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker + PND_FEATURES (22 cols), float64, no
        infinities. One row per (ticker, date) in the input panel.

    Spec References
    ----------------
    SPEC-MODEL-006: this is the data this pre-filter scores — must never
    use information not knowable same-day (see module docstring).
    SPEC-PIPE-004: fully vectorized via groupby/rolling.

    PIT Assumptions
    ----------------
    Same as features/technical.py: OHLCV/delivery is PITRule.NONE
    (same-day knowable). No feature here looks forward.

    Raises
    ------
    ValueError
        If `ohlcv` is missing any of REQUIRED_OHLCV_COLUMNS.
    """
    missing = [c for c in REQUIRED_OHLCV_COLUMNS if c not in ohlcv.columns]
    if missing:
        raise ValueError(f"ohlcv is missing required columns: {missing}")

    df = ohlcv.sort_values(["ticker", "date"]).reset_index(drop=True).copy()
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype(np.float64)
    df["volume"] = df["volume"].astype(np.float64)
    if "delivery_pct" not in df.columns:
        logger.debug("ohlcv has no delivery_pct column; delivery-based P&D features will be NaN")
        df["delivery_pct"] = np.nan
    else:
        df["delivery_pct"] = df["delivery_pct"].astype(np.float64)

    volume_out = _category_volume_anomalies(df)
    price_out = _category_price_anomalies(df)
    delivery_out = _category_delivery_collapse(df)
    micro_out = _category_microstructure(df)
    cross_out = _category_cross_feature(df, volume_out, price_out)

    result = pd.concat(
        [df[["date", "ticker"]], volume_out, price_out, delivery_out, micro_out, cross_out], axis=1
    )
    for col in PND_FEATURES:
        result[col] = result[col].astype(np.float64).replace([np.inf, -np.inf], np.nan)

    return result[["date", "ticker"] + PND_FEATURES]

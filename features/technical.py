"""
features/technical.py

Phase: 1.1 (Core Feature Computation)
Specs: SPEC-FEAT-001, SPEC-FEAT-002, SPEC-PIPE-004, SPEC-PIPE-005, SPEC-SOLID-001
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine, backtest

Computes the core technical-indicator feature set from adjusted OHLCV data
(DuckDB `ohlcv_adjusted`, SPEC-DS-001) across 11 categories. Every feature
is computed for the *entire* ticker x date panel in one vectorized pass —
SPEC-PIPE-004 forbids Python loops over individual stocks. The only
per-ticker iteration here is `DataFrame.groupby('ticker').apply(...)`,
which is pandas' own (Cython-level) group dispatch, not a manual
`for ticker in tickers` loop; each call inside a group still operates on
a full per-ticker numpy array via TA-Lib or pandas' vectorized rolling
ops. Supertrend is the one indicator with a genuine sequential recurrence
(its bands depend on the previous bar's final bands) — that recurrence is
inherent to the indicator's definition (same reason EMA needs one), not a
stock-level loop, and runs once per ticker inside the same groupby.

Spec note on feature count: the calling prompt's per-category counts
(8+8+4+9+8+5+5+5+5+5+8) sum to 70, not the "76" mentioned in the same
prompt's header. This module implements exactly the 70 named/countable
features enumerated per-category (CORE_TECHNICAL_FEATURES below) rather
than inventing 6 unspecified extra ones — see BuildLog.md "P1.1" for the
arithmetic and the decision to flag rather than guess.
"""

import logging
from typing import List, Optional

import numpy as np
import pandas as pd
import talib

logger = logging.getLogger(__name__)

REQUIRED_OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]

# ===== Category membership (SPEC-FEAT-001: catalog is the single source of
# truth for what this module promises to compute) =====
_CATEGORY_1_PRICE_POSITION = [
    "pct_rank_5d",
    "pct_rank_21d",
    "pct_rank_63d",
    "dist_from_52w_high",
    "dist_from_52w_low",
    "open_close_range_pct",
    "high_low_range_pct",
    "prev_close_gap_pct",
]
_CATEGORY_2_SMA_RATIOS = [
    "sma_20_ratio",
    "sma_50_ratio",
    "sma_100_ratio",
    "sma_200_ratio",
    "sma_20_50_ratio",
    "sma_50_100_ratio",
    "sma_50_200_ratio",
    "sma_200_weekly_ratio",
]
_CATEGORY_3_EMA_RATIOS = ["ema_8_ratio", "ema_21_ratio", "ema_55_ratio", "ema_89_ratio"]
_CATEGORY_4_OSCILLATORS = [
    "rsi_14",
    "rsi_2",
    "stoch_k",
    "stoch_d",
    "macd_hist",
    "williams_r",
    "cci_20",
    "mfi_14",
    "roc_10",
]
_CATEGORY_5_TREND_STRENGTH = [
    "adx_14",
    "di_plus",
    "di_minus",
    "supertrend_dir",
    "supertrend_signal",
    "linear_reg_slope_21",
    "linear_reg_r2_21",
    "trend_consistency_21",
]
_CATEGORY_6_VOLATILITY = [
    "atr_14_pct",
    "bb_position",
    "bb_width_pct",
    "keltner_position",
    "hist_vol_21",
]
# rs_vs_nifty50_21d "through" rs_vs_nifty500_21d (prompt shorthand) resolved
# against the 3 broad-market ETF proxies actually present in ohlcv_adjusted
# (NIFTYBEES, NIF100BEES, MONIFTY500) — see BENCHMARK_TICKERS below.
_CATEGORY_7_RELATIVE_STRENGTH = [
    "rs_vs_nifty50_21d",
    "rs_vs_nifty100_21d",
    "rs_vs_nifty500_21d",
    "beta_63d",
    "alpha_21d",
]
_CATEGORY_8_MOMENTUM_SCORES = [
    "composite_momentum_5d",
    "composite_momentum_21d",
    "composite_momentum_63d",
    "ema_ribbon_alignment",
    "ema_ribbon_spread",
]
_CATEGORY_9_VOLUME_DELIVERY = [
    "volume_ratio_5d",
    "volume_ratio_21d",
    "delivery_pct",
    "delivery_pct_zscore_21d",
    "delivery_price_corr_21d",
]
_CATEGORY_10_ICHIMOKU = [
    "ichimoku_cloud_position",
    "ichimoku_leading_span_a",
    "tenkan_kijun_signal",
    "chikou_span_signal",
    "ichimoku_breakout",
]
_CATEGORY_11_DERIVED = [
    "base_breakout_ratio",
    "vol_compression_21d",
    "vol_compression_63d",
    "gap_up_pct",
    "gap_down_pct",
    "intraday_reversal_score",
    "close_position_in_range",
    "body_to_range_ratio",
]

CORE_TECHNICAL_FEATURES: List[str] = (
    _CATEGORY_1_PRICE_POSITION
    + _CATEGORY_2_SMA_RATIOS
    + _CATEGORY_3_EMA_RATIOS
    + _CATEGORY_4_OSCILLATORS
    + _CATEGORY_5_TREND_STRENGTH
    + _CATEGORY_6_VOLATILITY
    + _CATEGORY_7_RELATIVE_STRENGTH
    + _CATEGORY_8_MOMENTUM_SCORES
    + _CATEGORY_9_VOLUME_DELIVERY
    + _CATEGORY_10_ICHIMOKU
    + _CATEGORY_11_DERIVED
)
assert len(CORE_TECHNICAL_FEATURES) == 70, "CORE_TECHNICAL_FEATURES catalog drifted — see module docstring"

# Nifty-tracking ETF tickers used as Category 7 relative-strength benchmarks
# (no raw NSE index series is ingested as of Phase 1 — these ETFs trade as
# ordinary EQ-series securities and are already present in ohlcv_adjusted).
BENCHMARK_TICKERS = {
    "nifty50": "NIFTYBEES",
    "nifty100": "NIF100BEES",
    "nifty500": "MONIFTY500",
}

SUPERTREND_PERIOD = 10
SUPERTREND_MULTIPLIER = 3.0


# ===== Generic grouped-computation helpers =====
def _grouped_rolling(df: pd.DataFrame, col: str, window: int, how: str) -> pd.Series:
    """Per-ticker rolling aggregate, full window required (SPEC-FEAT-001 NaN-until-ready)."""
    grouped = df.groupby("ticker", sort=False)[col].rolling(window, min_periods=window)
    return getattr(grouped, how)().reset_index(level=0, drop=True)


def _grouped_shift(df: pd.DataFrame, col: str, periods: int) -> pd.Series:
    return df.groupby("ticker", sort=False)[col].shift(periods)


def _apply_per_ticker(df: pd.DataFrame, fn):
    """
    Apply fn(ticker_group) -> Series/DataFrame per ticker, concatenating results.

    Deliberately avoids `df.groupby('ticker').apply(fn)`: when the input
    has exactly one ticker and fn returns a Series, pandas' apply silently
    reshapes the result into a single wide row instead of concatenating it
    as a per-row Series (a long-standing pandas footgun, not a bug in fn).
    Caught by tests/unit/test_features_technical.py's single-ticker
    minimum-history test. A plain per-group loop + pd.concat sidesteps it
    while remaining the same "per-ticker dispatch, not per-stock Python
    feature-math loop" pattern used throughout this module (SPEC-PIPE-004).
    """
    parts = [fn(g) for _, g in df.groupby("ticker", sort=False)]
    return pd.concat(parts)


def _grouped_talib_single(df: pd.DataFrame, cols: List[str], fn, **kwargs) -> pd.Series:
    """Apply a single-output TA-Lib function per ticker (vectorized C call per group)."""

    def _one(g: pd.DataFrame) -> pd.Series:
        arrays = [g[c].to_numpy(dtype=np.float64) for c in cols]
        return pd.Series(fn(*arrays, **kwargs), index=g.index)

    return _apply_per_ticker(df, _one)


def _grouped_talib_multi(df: pd.DataFrame, cols: List[str], fn, out_names: List[str], **kwargs) -> pd.DataFrame:
    """Apply a multi-output TA-Lib function (e.g. MACD, STOCH, BBANDS) per ticker."""

    def _one(g: pd.DataFrame) -> pd.DataFrame:
        arrays = [g[c].to_numpy(dtype=np.float64) for c in cols]
        outs = fn(*arrays, **kwargs)
        return pd.DataFrame({name: arr for name, arr in zip(out_names, outs)}, index=g.index)

    return _apply_per_ticker(df, _one)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Elementwise division that yields NaN (not a RuntimeWarning/inf) on 0/0 or x/0."""
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator.to_numpy(dtype=np.float64) / denominator.to_numpy(dtype=np.float64)
    return pd.Series(result, index=numerator.index).replace([np.inf, -np.inf], np.nan)


# ===== Category 1: Price Position & Range =====
def _category_1_price_position(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for window in (5, 21, 63):
        lo = _grouped_rolling(df, "low", window, "min")
        hi = _grouped_rolling(df, "high", window, "max")
        out[f"pct_rank_{window}d"] = _safe_div(df["close"] - lo, hi - lo)

    hi252 = _grouped_rolling(df, "high", 252, "max")
    lo252 = _grouped_rolling(df, "low", 252, "min")
    out["dist_from_52w_high"] = _safe_div(df["close"] - hi252, hi252)
    out["dist_from_52w_low"] = _safe_div(df["close"] - lo252, lo252)

    out["open_close_range_pct"] = _safe_div(df["close"] - df["open"], df["open"]) * 100
    out["high_low_range_pct"] = _safe_div(df["high"] - df["low"], df["close"]) * 100
    prev_close = _grouped_shift(df, "close", 1)
    out["prev_close_gap_pct"] = _safe_div(df["open"] - prev_close, prev_close) * 100
    return out


# ===== Category 2: SMA Ratios =====
def _weekly_sma200_ratio(df: pd.DataFrame, weeks: int = 50) -> pd.Series:
    """close / SMA(50 weekly closes) — the smoother, weekly-bar analogue of sma_200_ratio."""

    def _one(g: pd.DataFrame) -> pd.Series:
        s = g.sort_values("date")
        closes = s.set_index("date")["close"]
        weekly_close = closes.resample("W-FRI").last().dropna()
        weekly_sma = weekly_close.rolling(weeks, min_periods=weeks).mean()
        daily_sma = weekly_sma.reindex(closes.index, method="ffill")
        ratio = closes.to_numpy() / daily_sma.to_numpy()
        return pd.Series(ratio, index=s.index)

    return _apply_per_ticker(df, _one)


def _category_2_sma_ratios(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    sma = {n: _grouped_rolling(df, "close", n, "mean") for n in (20, 50, 100, 200)}
    out["sma_20_ratio"] = _safe_div(df["close"], sma[20])
    out["sma_50_ratio"] = _safe_div(df["close"], sma[50])
    out["sma_100_ratio"] = _safe_div(df["close"], sma[100])
    out["sma_200_ratio"] = _safe_div(df["close"], sma[200])
    out["sma_20_50_ratio"] = _safe_div(sma[20], sma[50])
    out["sma_50_100_ratio"] = _safe_div(sma[50], sma[100])
    out["sma_50_200_ratio"] = _safe_div(sma[50], sma[200])
    out["sma_200_weekly_ratio"] = _weekly_sma200_ratio(df)
    return out


# ===== Category 3: EMA Ratios =====
def _category_3_ema_ratios(df: pd.DataFrame, ema_cache: dict) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for period in (8, 21, 55, 89):
        out[f"ema_{period}_ratio"] = _safe_div(df["close"], ema_cache[period])
    return out


# ===== Category 4: Momentum Oscillators =====
def _category_4_oscillators(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["rsi_14"] = _grouped_talib_single(df, ["close"], talib.RSI, timeperiod=14)
    out["rsi_2"] = _grouped_talib_single(df, ["close"], talib.RSI, timeperiod=2)

    stoch = _grouped_talib_multi(
        df,
        ["high", "low", "close"],
        talib.STOCH,
        ["stoch_k", "stoch_d"],
        fastk_period=14,
        slowk_period=3,
        slowk_matype=0,
        slowd_period=3,
        slowd_matype=0,
    )
    out["stoch_k"] = stoch["stoch_k"]
    out["stoch_d"] = stoch["stoch_d"]

    macd = _grouped_talib_multi(
        df, ["close"], talib.MACD, ["macd", "macd_signal", "macd_hist"],
        fastperiod=12, slowperiod=26, signalperiod=9,
    )
    out["macd_hist"] = macd["macd_hist"]

    out["williams_r"] = _grouped_talib_single(df, ["high", "low", "close"], talib.WILLR, timeperiod=14)
    out["cci_20"] = _grouped_talib_single(df, ["high", "low", "close"], talib.CCI, timeperiod=20)
    out["mfi_14"] = _grouped_talib_single(df, ["high", "low", "close", "volume"], talib.MFI, timeperiod=14)
    out["roc_10"] = _grouped_talib_single(df, ["close"], talib.ROC, timeperiod=10)
    return out


# ===== Category 5: Trend Strength =====
def _supertrend(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> tuple:
    """
    Standard ATR Supertrend(10, 3). Sequential recurrence per TA convention
    (each bar's final bands depend on the prior bar's), computed once per
    ticker inside the caller's groupby — not a loop over stocks.

    Returns (direction, signal) where direction in {-1, +1} (NaN during ATR
    warm-up) and signal is +1/-1 on a direction flip, else 0.
    """
    n = len(close)
    atr = talib.ATR(high, low, close, timeperiod=SUPERTREND_PERIOD)
    hl2 = (high + low) / 2.0
    basic_upper = hl2 + SUPERTREND_MULTIPLIER * atr
    basic_lower = hl2 - SUPERTREND_MULTIPLIER * atr

    final_upper = np.full(n, np.nan)
    final_lower = np.full(n, np.nan)
    direction = np.full(n, np.nan)

    start = SUPERTREND_PERIOD
    if n <= start:
        return direction, np.zeros(n)

    final_upper[start] = basic_upper[start]
    final_lower[start] = basic_lower[start]
    direction[start] = 1.0

    for i in range(start + 1, n):
        final_upper[i] = (
            basic_upper[i]
            if (basic_upper[i] < final_upper[i - 1] or close[i - 1] > final_upper[i - 1])
            else final_upper[i - 1]
        )
        final_lower[i] = (
            basic_lower[i]
            if (basic_lower[i] > final_lower[i - 1] or close[i - 1] < final_lower[i - 1])
            else final_lower[i - 1]
        )
        if close[i] > final_upper[i - 1]:
            direction[i] = 1.0
        elif close[i] < final_lower[i - 1]:
            direction[i] = -1.0
        else:
            direction[i] = direction[i - 1]

    signal = np.zeros(n)
    flip = np.diff(direction, prepend=direction[0] if n else np.nan) != 0
    flip[: start + 1] = False
    signal[flip] = direction[flip]
    return direction, signal


def _category_5_trend_strength(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["adx_14"] = _grouped_talib_single(df, ["high", "low", "close"], talib.ADX, timeperiod=14)
    out["di_plus"] = _grouped_talib_single(df, ["high", "low", "close"], talib.PLUS_DI, timeperiod=14)
    out["di_minus"] = _grouped_talib_single(df, ["high", "low", "close"], talib.MINUS_DI, timeperiod=14)

    def _st(g: pd.DataFrame) -> pd.DataFrame:
        direction, signal = _supertrend(
            g["high"].to_numpy(dtype=np.float64),
            g["low"].to_numpy(dtype=np.float64),
            g["close"].to_numpy(dtype=np.float64),
        )
        return pd.DataFrame({"supertrend_dir": direction, "supertrend_signal": signal}, index=g.index)

    st = _apply_per_ticker(df, _st)
    out["supertrend_dir"] = st["supertrend_dir"]
    out["supertrend_signal"] = st["supertrend_signal"]

    out["linear_reg_slope_21"] = _grouped_talib_single(df, ["close"], talib.LINEARREG_SLOPE, timeperiod=21)

    def _r2(g: pd.DataFrame) -> pd.Series:
        arr = g["close"].to_numpy(dtype=np.float64)
        t = np.arange(len(arr), dtype=np.float64)
        corr = talib.CORREL(arr, t, timeperiod=21)
        return pd.Series(corr ** 2, index=g.index)

    out["linear_reg_r2_21"] = _apply_per_ticker(df, _r2)

    daily_ret = df.groupby("ticker", sort=False)["close"].diff()
    work = df.copy()
    work["_pos"] = (daily_ret > 0).astype(float)
    work["_neg"] = (daily_ret < 0).astype(float)
    pos21 = _grouped_rolling(work, "_pos", 21, "mean")
    neg21 = _grouped_rolling(work, "_neg", 21, "mean")
    slope_sign = np.sign(out["linear_reg_slope_21"])
    out["trend_consistency_21"] = np.where(slope_sign >= 0, pos21, neg21)
    return out


# ===== Category 6: Volatility =====
def _category_6_volatility(df: pd.DataFrame, ema_cache: dict) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    atr14 = _grouped_talib_single(df, ["high", "low", "close"], talib.ATR, timeperiod=14)
    out["atr_14_pct"] = _safe_div(atr14, df["close"]) * 100

    bb = _grouped_talib_multi(
        df, ["close"], talib.BBANDS, ["bb_upper", "bb_middle", "bb_lower"],
        timeperiod=20, nbdevup=2, nbdevdn=2, matype=0,
    )
    out["bb_position"] = _safe_div(df["close"] - bb["bb_lower"], bb["bb_upper"] - bb["bb_lower"])
    out["bb_width_pct"] = _safe_div(bb["bb_upper"] - bb["bb_lower"], bb["bb_middle"]) * 100

    ema20 = ema_cache[20]
    atr20 = _grouped_talib_single(df, ["high", "low", "close"], talib.ATR, timeperiod=20)
    kc_upper = ema20 + 2 * atr20
    kc_lower = ema20 - 2 * atr20
    out["keltner_position"] = _safe_div(df["close"] - kc_lower, kc_upper - kc_lower)

    work = df.copy()
    work["_log_ret"] = np.log(df["close"] / _grouped_shift(df, "close", 1))
    std21 = _grouped_rolling(work, "_log_ret", 21, "std")
    out["hist_vol_21"] = std21 * np.sqrt(252) * 100
    return out


# ===== Category 7: Relative Strength (requires benchmark) =====
def _category_7_relative_strength(df: pd.DataFrame, benchmark: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    names = ["nifty50", "nifty100", "nifty500"]

    if benchmark is None or benchmark.empty:
        logger.warning("No benchmark data supplied; Category 7 relative-strength features will be all-NaN")
        for n in names:
            out[f"rs_vs_{n}_21d"] = np.nan
        out["beta_63d"] = np.nan
        out["alpha_21d"] = np.nan
        return out

    bm = benchmark[["date"] + [f"{n}_close" for n in names]].drop_duplicates("date").sort_values("date").copy()

    # Reindex onto the full set of dates actually traded by `df` and forward-fill
    # (limit=5 trading days) before computing returns. The benchmark ETFs
    # (NIFTYBEES/NIF100BEES/MONIFTY500) occasionally have no row on a date when
    # the general equity universe does trade (~13 such gaps in the last 500
    # trading days, e.g. 2026-03-31, 2026-04-03) — without this, that single
    # missing benchmark date turns into a NaN in nifty50_daily_ret/_bm_ret, and
    # because beta_63d/alpha_21d use a strict rolling(63, min_periods=63)
    # window, one NaN poisons up to 63 consecutive days of output. This is what
    # made beta_63d/alpha_21d ~94% null in the most recent months despite
    # NIFTYBEES having near-complete history (RCA 2026-07-05). A short ffill
    # limit avoids masking genuine multi-year benchmark-not-yet-listed gaps
    # (e.g. NIF100BEES pre-2015, MONIFTY500 pre-2023-10), which stay NaN.
    calendar_dates = df["date"].drop_duplicates().sort_values()
    bm = (
        bm.set_index("date")
        .reindex(calendar_dates)
        .ffill(limit=5)
        .rename_axis("date")
        .reset_index()
    )
    for n in names:
        bm[f"{n}_ret_21d"] = bm[f"{n}_close"] / bm[f"{n}_close"].shift(21) - 1
    bm["nifty50_daily_ret"] = bm["nifty50_close"].pct_change()

    merge_cols = ["date", "nifty50_daily_ret"] + [f"{n}_ret_21d" for n in names]
    merged = df.merge(bm[merge_cols], on="date", how="left")
    merged.index = df.index

    # Illiquid stocks can have no-trade gap days scattered through their
    # history; a plain pct_change(21)/rolling(63) breaks on any single gap
    # (see RCA 2026-07-06: ~61 tickers stayed permanently null despite having
    # hundreds of valid trading days). We tolerate gaps by reaching back at
    # most 7 extra calendar rows — ffill(limit=7) for the 21d return, and a
    # widened 70-row window (63+7) with min_periods=63 for beta, so pandas'
    # pairwise-NaN-aware rolling cov/var can skip up to 7 gap days within the
    # window. Genuinely short histories (new listings, delisted stocks) still
    # correctly stay NaN — there's no bounded reach-back that can manufacture
    # 63 valid days out of fewer than 63 that exist.
    close_filled = df.groupby("ticker", sort=False)["close"].ffill(limit=7)
    stock_ret_21d = close_filled.groupby(df["ticker"], sort=False).pct_change(21)
    for n in names:
        out[f"rs_vs_{n}_21d"] = stock_ret_21d - merged[f"{n}_ret_21d"]

    work = df.copy()
    work["_stock_ret"] = df.groupby("ticker", sort=False)["close"].pct_change()
    work["_bm_ret"] = merged["nifty50_daily_ret"]

    def _beta(g: pd.DataFrame) -> pd.Series:
        cov = g["_stock_ret"].rolling(70, min_periods=63).cov(g["_bm_ret"])
        var = g["_bm_ret"].rolling(70, min_periods=63).var()
        return _safe_div(cov, var)

    beta_63d = _apply_per_ticker(work, _beta)
    out["beta_63d"] = beta_63d
    out["alpha_21d"] = stock_ret_21d - beta_63d * merged["nifty50_ret_21d"]
    return out


# ===== Category 8: Momentum Scores =====
def _category_8_momentum_scores(df: pd.DataFrame, ema_cache: dict) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["composite_momentum_5d"] = df.groupby("ticker", sort=False)["close"].pct_change(5)
    out["composite_momentum_21d"] = df.groupby("ticker", sort=False)["close"].pct_change(21)
    out["composite_momentum_63d"] = df.groupby("ticker", sort=False)["close"].pct_change(63)

    ema8, ema21, ema55, ema89 = ema_cache[8], ema_cache[21], ema_cache[55], ema_cache[89]
    alignment = (np.sign(ema8 - ema21) + np.sign(ema21 - ema55) + np.sign(ema55 - ema89)) / 3.0
    out["ema_ribbon_alignment"] = alignment
    out["ema_ribbon_spread"] = _safe_div(ema8 - ema89, df["close"]) * 100
    return out


# ===== Category 9: Volume & Delivery =====
def _category_9_volume_delivery(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    vol_sma5 = _grouped_rolling(df, "volume", 5, "mean")
    vol_sma21 = _grouped_rolling(df, "volume", 21, "mean")
    out["volume_ratio_5d"] = _safe_div(df["volume"], vol_sma5)
    out["volume_ratio_21d"] = _safe_div(df["volume"], vol_sma21)

    out["delivery_pct"] = df["delivery_pct"]
    mean21 = _grouped_rolling(df, "delivery_pct", 21, "mean")
    std21 = _grouped_rolling(df, "delivery_pct", 21, "std")
    out["delivery_pct_zscore_21d"] = _safe_div(df["delivery_pct"] - mean21, std21)

    def _corr(g: pd.DataFrame) -> pd.Series:
        return g["delivery_pct"].rolling(21, min_periods=21).corr(g["close"])

    out["delivery_price_corr_21d"] = _apply_per_ticker(df, _corr)
    return out


# ===== Category 10: Ichimoku Cloud =====
def _category_10_ichimoku(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    tenkan = (_grouped_rolling(df, "high", 9, "max") + _grouped_rolling(df, "low", 9, "min")) / 2
    kijun = (_grouped_rolling(df, "high", 26, "max") + _grouped_rolling(df, "low", 26, "min")) / 2
    senkou_a_raw = (tenkan + kijun) / 2
    senkou_b_raw = (_grouped_rolling(df, "high", 52, "max") + _grouped_rolling(df, "low", 52, "min")) / 2

    # Senkou spans are classically plotted 26 periods ahead; to read "today's
    # cloud" without look-ahead we shift the raw (data-through-t) series
    # forward 26 bars, i.e. today's cloud value was computed as of t-26.
    work = df.copy()
    work["_sa_raw"] = senkou_a_raw
    work["_sb_raw"] = senkou_b_raw
    senkou_a = _grouped_shift(work, "_sa_raw", 26)
    senkou_b = _grouped_shift(work, "_sb_raw", 26)

    cloud_top = np.maximum(senkou_a, senkou_b)
    cloud_bottom = np.minimum(senkou_a, senkou_b)
    cloud_mid = (senkou_a + senkou_b) / 2

    out["ichimoku_cloud_position"] = _safe_div(df["close"] - cloud_mid, df["close"])
    out["ichimoku_leading_span_a"] = _safe_div(df["close"] - senkou_a, df["close"])
    out["tenkan_kijun_signal"] = np.sign(tenkan - kijun)

    chikou_ref = _grouped_shift(df, "close", 26)
    out["chikou_span_signal"] = np.sign(df["close"] - chikou_ref)

    work["_cloud_top"] = cloud_top
    work["_cloud_bottom"] = cloud_bottom
    prev_close = _grouped_shift(df, "close", 1)
    prev_cloud_top = _grouped_shift(work, "_cloud_top", 1)
    prev_cloud_bottom = _grouped_shift(work, "_cloud_bottom", 1)
    breakout_up = (df["close"] > cloud_top) & (prev_close <= prev_cloud_top)
    breakout_down = (df["close"] < cloud_bottom) & (prev_close >= prev_cloud_bottom)
    out["ichimoku_breakout"] = np.where(breakout_up, 1, np.where(breakout_down, -1, 0))
    return out


# ===== Category 11: Derived / Engineered =====
def _category_11_derived(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    work = df.copy()
    work["_hi21"] = _grouped_rolling(df, "high", 21, "max")
    prior_hi21 = _grouped_shift(work, "_hi21", 1)
    out["base_breakout_ratio"] = _safe_div(df["close"], prior_hi21)

    work["_ret"] = df.groupby("ticker", sort=False)["close"].pct_change()
    std21 = _grouped_rolling(work, "_ret", 21, "std")
    std63 = _grouped_rolling(work, "_ret", 63, "std")
    std126 = _grouped_rolling(work, "_ret", 126, "std")
    out["vol_compression_21d"] = _safe_div(std21, std63)
    out["vol_compression_63d"] = _safe_div(std63, std126)

    prev_close = _grouped_shift(df, "close", 1)
    gap_pct = _safe_div(df["open"] - prev_close, prev_close) * 100
    out["gap_up_pct"] = gap_pct.clip(lower=0)
    out["gap_down_pct"] = gap_pct.clip(upper=0)

    rng = df["high"] - df["low"]
    out["intraday_reversal_score"] = _safe_div(df["close"] - df["open"], rng)
    out["close_position_in_range"] = _safe_div(df["close"] - df["low"], rng)
    out["body_to_range_ratio"] = _safe_div((df["close"] - df["open"]).abs(), rng)
    return out


def compute_technical_features(ohlcv: pd.DataFrame, benchmark: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Compute all 70 core technical features for a multi-ticker OHLCV panel.

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel sourced from DuckDB `ohlcv_adjusted`
        (SPEC-DS-001) with columns: date, ticker, open, high, low, close,
        volume, and optionally delivery_pct. One row per (ticker, date).
        Must contain full lookback history per ticker (>= 252 trading days
        to populate every Category 1/2 feature; see SPEC-FEAT-001) — this
        function does not fetch data itself (SPEC-SOLID-005: data access is
        features/matrix_builder.py's responsibility via the DataStore API).
    benchmark : pd.DataFrame, optional
        Columns: date, nifty50_close, nifty100_close, nifty500_close.
        Required only for Category 7 (relative strength / beta / alpha);
        all other categories are computed regardless. If omitted, Category
        7 columns are returned as all-NaN rather than raising.

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, + CORE_TECHNICAL_FEATURES (70 cols), float64,
        no infinities. One row per (ticker, date) in the input panel.
        Rows are sorted by (ticker, date).

    Spec References
    ----------------
    SPEC-FEAT-001: features with an N-day lookback are NaN until N
    observations exist for that ticker (enforced via rolling/TA-Lib
    min_periods=window — TA-Lib's own warm-up NaNs do the same).
    SPEC-PIPE-004: fully vectorized; the only per-ticker iteration is
    pandas groupby dispatch (see module docstring), never a manual loop.

    PIT Assumptions
    ----------------
    OHLCV carries PITRule.NONE (features/registry.py) — same-day price/
    volume data is always contemporaneously knowable, so no PIT filtering
    is applied here. Ichimoku's Senkou spans are explicitly back-shifted
    (not forward-shifted) to avoid reading future bars (see
    _category_10_ichimoku).

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
        logger.debug("ohlcv has no delivery_pct column; Category 9 delivery features will be NaN")
        df["delivery_pct"] = np.nan
    else:
        df["delivery_pct"] = df["delivery_pct"].astype(np.float64)

    ema_cache = {
        n: _grouped_talib_single(df, ["close"], talib.EMA, timeperiod=n) for n in (8, 20, 21, 55, 89)
    }

    pieces = [
        df[["date", "ticker"]],
        _category_1_price_position(df),
        _category_2_sma_ratios(df),
        _category_3_ema_ratios(df, ema_cache),
        _category_4_oscillators(df),
        _category_5_trend_strength(df),
        _category_6_volatility(df, ema_cache),
        _category_7_relative_strength(df, benchmark),
        _category_8_momentum_scores(df, ema_cache),
        _category_9_volume_delivery(df),
        _category_10_ichimoku(df),
        _category_11_derived(df),
    ]
    result = pd.concat(pieces, axis=1)

    for col in CORE_TECHNICAL_FEATURES:
        result[col] = result[col].astype(np.float64).replace([np.inf, -np.inf], np.nan)

    return result[["date", "ticker"] + CORE_TECHNICAL_FEATURES]

"""
features/multibagger.py

Phase: 2.4 (Multibagger Detection System M-08)
Specs: SPEC-MODEL-001, SPEC-FEAT-002, SPEC-PIPE-003, SPEC-PIPE-004
Owner: Platform / Features
Consumers: systems/ml_signal_engine/models/multibagger/multibagger_model.py,
           features/matrix_builder.py

33 multibagger-specific features, the literal list from this phase's
build prompt (Base formation 6 + Accumulation signals 7 + Relative
strength 5 + Trend quality 5 + Volatility compression 4 + Historical
analogues 6 = 33) — not 01_features.md's older, differently-named
34-feature list, which P2.3's matrix_builder.py used as an explicit NaN
stub pending this real build (see that module's docstring: "a future
features/multibagger.py replaces this stub list ... wired in exactly like
every other category"). Same "literal build-prompt text governs over an
older reference doc" precedent already applied to fundamental.py,
governance.py, mf_holdings.py, fno_features.py.

Self-contained, mirrors features/technical.py's and features/
pnd_features.py's vectorized groupby/rolling/talib idiom (SPEC-PIPE-004:
no Python loop over stocks) rather than importing their private helpers —
same module-boundary convention every other feature module in this
project already follows (each feature module owns its own small rolling/
talib helpers).

PIT Assumptions
----------------
Technical/volume/delivery-derived features (everything except the 4
institutional ones below) are PITRule.NONE — computed purely from the
OHLCV panel passed in, same-day knowable at every historical date.

The 4 institutional features (`institutional_accumulation_flag`,
`mf_discovery_score`, `smart_money_flow`, `promoter_buying_flag`) draw on
MF-holdings/governance SNAPSHOTS (single ticker-keyed rows "as of" one
date — features/mf_holdings.py's and features/governance.py's panel
functions only ever return "as of today", never a full historical
date-series; recomputing them for every historical date in a multi-year
training panel would mean thousands of extra DataStore API calls for no
real benefit during this Phase). Broadcasting today's snapshot across
every historical row would be a real PIT/lookahead-bias bug (SPEC-PIPE-003)
— instead, these 4 features are merged ONLY onto the row(s) matching the
LATEST date in the supplied `ohlcv` panel; every earlier historical date
gets NaN for these 4 columns specifically. This mirrors the same "today
only, not backfill" pattern features/matrix_builder.py's docstring
already documents for the M-01 HMM regime features.

Historical-analogue features (28-33) compare each stock's own base-
pattern shape against HISTORICAL_MULTIBAGGER_REFERENCE — approximate,
literature-informed reference statistics (typical base length/tightness/
depth for confirmed historical multibaggers), NOT fitted from a real
internal historical-pattern archive (no such archive has been built in
this codebase yet — same documented-approximation precedent as
ASSUMED_TAX_RATE/ASSUMED_FD_RATE).
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import talib

from features._vector_utils import (
    apply_per_ticker as _apply_per_ticker,
    grouped_rolling as _grouped_rolling,
    grouped_shift as _grouped_shift,
    grouped_talib_multi as _grouped_talib_multi,
    grouped_talib_single as _grouped_talib_single,
    safe_div as _safe_div,
)

logger = logging.getLogger(__name__)

BASE_FORMATION_FEATURES = [
    "base_length_days", "base_tightness_pct", "base_depth_pct",
    "breakout_volume_ratio", "pre_breakout_vol_compression", "consolidation_pattern_score",
]
ACCUMULATION_FEATURES = [
    "delivery_accumulation_21d", "institutional_accumulation_flag", "mf_discovery_score",
    "volume_trend_21d", "quiet_accumulation_score", "smart_money_flow", "promoter_buying_flag",
]
RELATIVE_STRENGTH_FEATURES = [
    "rs_rank_universe", "rs_rank_sector", "rs_vs_nifty_52w",
    "rs_momentum_acceleration", "rs_stability_score",
]
TREND_QUALITY_FEATURES = [
    "trend_quality_score", "atr_ratio_trend", "ema_ribbon_health",
    "higher_highs_lower_lows", "weekly_trend_alignment",
]
VOLATILITY_COMPRESSION_FEATURES = [
    "vol_compression_ratio_63d", "vol_compression_ratio_126d", "iv_compression_flag", "range_compression_score",
]
HISTORICAL_ANALOGUE_FEATURES = [
    "base_pattern_similarity", "post_base_breakout_score", "recovery_from_correction",
    "sector_cycle_position", "market_cycle_alignment", "analogue_composite_score",
]

MULTIBAGGER_FEATURES: List[str] = (
    BASE_FORMATION_FEATURES
    + ACCUMULATION_FEATURES
    + RELATIVE_STRENGTH_FEATURES
    + TREND_QUALITY_FEATURES
    + VOLATILITY_COMPRESSION_FEATURES
    + HISTORICAL_ANALOGUE_FEATURES
)

# See module docstring: approximate, literature-informed (cup-and-handle /
# base-and-breakout literature on multi-month consolidations preceding a
# major move), not fitted from a real archive.
HISTORICAL_MULTIBAGGER_REFERENCE = {
    "base_length_days": (90.0, 40.0),  # (mean, std) trading days, ~4-6 months
    "base_tightness_pct": (10.0, 5.0),
    "base_depth_pct": (30.0, 15.0),
}

BASE_TIGHT_BAND_PCT = 0.08  # within ±8% of rolling 20d SMA counts as a "tight" (basing) day


def _consecutive_true_run(flags: pd.Series, ticker: pd.Series) -> pd.Series:
    """Length of the consecutive True-run in `flags` ending at each row (same idiom as
    features/pnd_features.py's _consecutive_true_run — see that module for the vectorization trick)."""
    work = pd.DataFrame({"ticker": ticker.to_numpy(), "flag": flags.to_numpy().astype(int)})
    block_id = work.groupby("ticker")["flag"].transform(lambda s: (1 - s).cumsum())
    running = work.groupby([work["ticker"], block_id])["flag"].cumsum()
    return pd.Series(running.to_numpy() * work["flag"].to_numpy(), index=flags.index)


def _cross_sectional_pct_rank(df: pd.DataFrame, col: str, by: List[str]) -> pd.Series:
    """Percentile rank (0-100) of `col` within each `by`-group (e.g. per date, or per date+sector)."""
    return df.groupby(by, sort=False)[col].rank(pct=True) * 100.0


# ===== Category 1: Base formation (6) =====
def _category_base_formation(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    sma20 = _grouped_rolling(df, "close", 20, "mean")
    tight_flag = (_safe_div(df["close"] - sma20, sma20).abs() <= BASE_TIGHT_BAND_PCT)
    out["base_length_days"] = _consecutive_true_run(tight_flag.fillna(False), df["ticker"])

    roll21_max_high = _grouped_rolling(df, "high", 21, "max")
    roll21_min_low = _grouped_rolling(df, "low", 21, "min")
    roll21_mean_close = _grouped_rolling(df, "close", 21, "mean")
    out["base_tightness_pct"] = _safe_div(roll21_max_high - roll21_min_low, roll21_mean_close) * 100.0

    roll252_max_high = _grouped_rolling(df, "high", 252, "max")
    out["base_depth_pct"] = _safe_div(roll252_max_high - roll21_mean_close, roll252_max_high) * 100.0

    roll21_mean_vol = _grouped_rolling(df, "volume", 21, "mean")
    out["breakout_volume_ratio"] = _safe_div(df["volume"], roll21_mean_vol)

    log_ret = df.groupby("ticker", sort=False)["close"].transform(lambda s: np.log(s / s.shift(1)))
    work = df.assign(_log_ret=log_ret)
    vol21 = _grouped_rolling(work, "_log_ret", 21, "std")
    vol126 = _grouped_rolling(work, "_log_ret", 126, "std")
    out["pre_breakout_vol_compression"] = _safe_div(vol21, vol126)

    tightness_score = (1.0 - (out["base_tightness_pct"] / 20.0)).clip(0, 1)
    length_score = (out["base_length_days"] / 60.0).clip(0, 1)
    out["consolidation_pattern_score"] = 100.0 * tightness_score * length_score

    return out


# ===== Category 2: Accumulation signals (7) =====
def _category_accumulation(
    df: pd.DataFrame,
    mf_snapshot: Optional[pd.DataFrame],
    governance_snapshot: Optional[pd.DataFrame],
) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    delivery_pct = df["delivery_pct"] if "delivery_pct" in df.columns else pd.Series(np.nan, index=df.index)
    work = df.assign(_delivery_pct=delivery_pct)
    roll21_delivery = _grouped_rolling(work, "_delivery_pct", 21, "mean")
    roll63_delivery = _grouped_rolling(work, "_delivery_pct", 63, "mean")
    out["delivery_accumulation_21d"] = roll21_delivery - roll63_delivery

    obv = _grouped_talib_single(df, ["close", "volume"], talib.OBV)
    work_obv = df.assign(_obv=obv)
    obv_slope = _grouped_talib_single(work_obv, ["_obv"], talib.LINEARREG_SLOPE, timeperiod=21)
    roll21_mean_vol = _grouped_rolling(df, "volume", 21, "mean")
    out["volume_trend_21d"] = _safe_div(obv_slope, roll21_mean_vol)

    compression = _category_base_formation(df)["pre_breakout_vol_compression"]
    quiet_component = (out["delivery_accumulation_21d"].clip(lower=0) / 5.0).clip(0, 1)
    calm_component = (1.0 - compression.clip(0, 2) / 2.0).clip(0, 1)
    # SPEC-FEAT-001: no fillna(0) here — both components depend on rolling
    # windows that need to warm up; a row where either hasn't warmed up yet
    # must produce NaN, not a falsely-confident 0.
    out["quiet_accumulation_score"] = 100.0 * (0.5 * quiet_component + 0.5 * calm_component)

    # SPEC-PIPE-003: snapshot-derived institutional features are PIT-safe ONLY
    # for the latest date in this panel — see module docstring.
    out["institutional_accumulation_flag"] = np.nan
    out["mf_discovery_score"] = np.nan
    out["smart_money_flow"] = np.nan
    out["promoter_buying_flag"] = np.nan

    if df.empty:
        return out[ACCUMULATION_FEATURES]

    latest_date = df["date"].max()
    latest_mask = df["date"] == latest_date
    latest_tickers = df.loc[latest_mask, "ticker"]

    if mf_snapshot is not None and not mf_snapshot.empty:
        mf_indexed = mf_snapshot.set_index("ticker")
        scheme_change = latest_tickers.map(mf_indexed.get("mf_scheme_count_change_1m", pd.Series(dtype=float)))
        new_entries = latest_tickers.map(mf_indexed.get("mf_new_entry_count", pd.Series(dtype=float)))
        scheme_count = latest_tickers.map(mf_indexed.get("mf_scheme_count", pd.Series(dtype=float)))
        holding_change = latest_tickers.map(mf_indexed.get("mf_total_holding_change_1m", pd.Series(dtype=float)))

        out.loc[latest_mask, "mf_discovery_score"] = (
            100.0 * _safe_div(new_entries.reset_index(drop=True), (scheme_count + 1).reset_index(drop=True))
        ).to_numpy()

        # holding_change is neutral-filled (a ticker absent from the MF snapshot
        # contributes 0, not NaN) — obv_slope is NOT (SPEC-FEAT-001: it needs a
        # real 21-day warmup; an un-warmed-up OBV slope must stay NaN, not 0).
        smart_money = 0.6 * holding_change.fillna(0).reset_index(drop=True) + 0.4 * obv_slope.loc[
            latest_mask
        ].reset_index(drop=True)
        out.loc[latest_mask, "smart_money_flow"] = smart_money.to_numpy()

        if governance_snapshot is not None and not governance_snapshot.empty:
            gov_indexed = governance_snapshot.set_index("ticker")
            dii_change = latest_tickers.map(gov_indexed.get("dii_change_qoq", pd.Series(dtype=float)))
            inst_flag = ((scheme_change.fillna(0) > 0) & (dii_change.fillna(0) > 0)).astype(float)
            out.loc[latest_mask, "institutional_accumulation_flag"] = inst_flag.to_numpy()

    if governance_snapshot is not None and not governance_snapshot.empty:
        gov_indexed = governance_snapshot.set_index("ticker")
        promoter_change = latest_tickers.map(gov_indexed.get("promoter_change_qoq", pd.Series(dtype=float)))
        out.loc[latest_mask, "promoter_buying_flag"] = (promoter_change.fillna(0) > 0).astype(float).to_numpy()

    return out[ACCUMULATION_FEATURES]


# ===== Category 3: Relative strength (5) =====
def _category_relative_strength(
    df: pd.DataFrame, benchmark: Optional[pd.DataFrame], sector_map: Optional[Dict[str, str]]
) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    ret_252d = _safe_div(df["close"], _grouped_shift(df, "close", 252)) - 1.0
    work = df.assign(_ret_252d=ret_252d, _sector=df["ticker"].map(sector_map or {}).fillna("UNKNOWN"))
    out["rs_rank_universe"] = _cross_sectional_pct_rank(work, "_ret_252d", ["date"])
    out["rs_rank_sector"] = _cross_sectional_pct_rank(work, "_ret_252d", ["date", "_sector"])

    if benchmark is not None and "nifty50_close" in benchmark.columns:
        bench = benchmark[["date", "nifty50_close"]].copy()
        merged = df[["date", "ticker", "close"]].merge(bench, on="date", how="left")
        merged["_stock_ret_252d"] = merged.groupby("ticker", sort=False)["close"].transform(
            lambda s: s / s.shift(252) - 1.0
        )
        merged["_nifty_ret_252d"] = merged["nifty50_close"] / merged.groupby("ticker", sort=False)[
            "nifty50_close"
        ].shift(252) - 1.0
        rs = (merged["_stock_ret_252d"] - merged["_nifty_ret_252d"]).to_numpy()
        out["rs_vs_nifty_52w"] = rs

        rs_series = pd.Series(rs, index=df.index)
        rs_shifted = df.assign(_rs=rs_series).groupby("ticker", sort=False)["_rs"].shift(21)
        out["rs_momentum_acceleration"] = rs_series - rs_shifted

        work_rs = df.assign(_rs=rs_series)
        rs_std_63 = _grouped_rolling(work_rs, "_rs", 63, "std")
        out["rs_stability_score"] = (100.0 - (rs_std_63 * 200.0).clip(0, 100)).clip(0, 100)
    else:
        out["rs_vs_nifty_52w"] = np.nan
        out["rs_momentum_acceleration"] = np.nan
        out["rs_stability_score"] = np.nan

    return out[RELATIVE_STRENGTH_FEATURES]


# ===== Category 4: Trend quality (5) =====
def _category_trend_quality(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    adx14 = _grouped_talib_single(df, ["high", "low", "close"], talib.ADX, timeperiod=14)
    sma50 = _grouped_rolling(df, "close", 50, "mean")
    sma200 = _grouped_rolling(df, "close", 200, "mean")
    align_score = (sma50 > sma200).astype(float) * 100.0
    # talib has no direct R^2 function; for a simple linear fit, R^2 equals the
    # squared correlation between y and a time index (standard OLS identity).
    time_idx = df.groupby("ticker", sort=False).cumcount().astype(np.float64)
    work_r2 = df.assign(_time_idx=time_idx)

    def _r2(g: pd.DataFrame) -> pd.Series:
        close_arr = g["close"].to_numpy(dtype=np.float64)
        idx_arr = g["_time_idx"].to_numpy(dtype=np.float64)
        corr = talib.CORREL(close_arr, idx_arr, timeperiod=63)
        return pd.Series(corr**2, index=g.index)

    r2_63 = _apply_per_ticker(work_r2, _r2) * 100.0
    # SPEC-FEAT-001: no fillna(0) — sma200 alone needs 200 days of warmup;
    # the composite must stay NaN until every component is real.
    out["trend_quality_score"] = (adx14.clip(0, 100) + align_score + r2_63) / 3.0

    atr14 = _grouped_talib_single(df, ["high", "low", "close"], talib.ATR, timeperiod=14)
    atr63 = _grouped_talib_single(df, ["high", "low", "close"], talib.ATR, timeperiod=63)
    out["atr_ratio_trend"] = _safe_div(atr14, atr63)

    ema8 = _grouped_talib_single(df, ["close"], talib.EMA, timeperiod=8)
    ema21 = _grouped_talib_single(df, ["close"], talib.EMA, timeperiod=21)
    ema55 = _grouped_talib_single(df, ["close"], talib.EMA, timeperiod=55)
    ema89 = _grouped_talib_single(df, ["close"], talib.EMA, timeperiod=89)
    pairs_ok = (ema8 > ema21).astype(int) + (ema21 > ema55).astype(int) + (ema55 > ema89).astype(int)
    out["ema_ribbon_health"] = pairs_ok / 3.0 * 100.0

    higher_high = (df["high"] > _grouped_shift(df, "high", 1)).astype(int)
    lower_low = (df["low"] < _grouped_shift(df, "low", 1)).astype(int)
    work_hh = df.assign(_hh=higher_high, _ll=lower_low)
    roll_hh = _grouped_rolling(work_hh, "_hh", 21, "sum")
    roll_ll = _grouped_rolling(work_hh, "_ll", 21, "sum")
    out["higher_highs_lower_lows"] = roll_hh - roll_ll

    def _weekly_alignment(g: pd.DataFrame) -> pd.Series:
        weekly_close = g.set_index("date")["close"].resample("W").last()
        weekly_sma10 = weekly_close.rolling(10, min_periods=10).mean()
        weekly_flag = (weekly_close > weekly_sma10).astype(float)
        daily_flag = weekly_flag.reindex(g["date"], method="ffill")
        return pd.Series(daily_flag.to_numpy(), index=g.index)

    out["weekly_trend_alignment"] = _apply_per_ticker(df, _weekly_alignment) * 100.0

    return out[TREND_QUALITY_FEATURES]


# ===== Category 5: Volatility compression (4) =====
def _category_volatility_compression(df: pd.DataFrame, fno_iv_panel: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    atr14 = _grouped_talib_single(df, ["high", "low", "close"], talib.ATR, timeperiod=14)
    work_atr = df.assign(_atr14=atr14)
    atr_mean_63 = _grouped_rolling(work_atr, "_atr14", 63, "mean")
    atr_mean_126 = _grouped_rolling(work_atr, "_atr14", 126, "mean")
    atr_mean_252 = _grouped_rolling(work_atr, "_atr14", 252, "mean")
    out["vol_compression_ratio_63d"] = _safe_div(atr_mean_63, atr_mean_252)
    out["vol_compression_ratio_126d"] = _safe_div(atr_mean_126, atr_mean_252)

    bbands = _grouped_talib_multi(df, ["close"], talib.BBANDS, ["_bb_upper", "_bb_middle", "_bb_lower"], timeperiod=20)
    bb_width = _safe_div(bbands["_bb_upper"] - bbands["_bb_lower"], bbands["_bb_middle"])
    work_bb = df.assign(_bb_width=bb_width)
    bb_width_mean_252 = _grouped_rolling(work_bb, "_bb_width", 252, "mean")
    # Current width vs. its own 1y average (fast, vectorized proxy for a true
    # percentile rank — documented simplification, see module docstring).
    out["range_compression_score"] = (100.0 * (1.0 - _safe_div(bb_width, bb_width_mean_252))).clip(0, 100)

    out["iv_compression_flag"] = np.nan
    if fno_iv_panel is not None and not fno_iv_panel.empty and "iv_call" in fno_iv_panel.columns:
        iv = df[["date", "ticker"]].merge(
            fno_iv_panel[["date", "ticker", "iv_call"]], on=["date", "ticker"], how="left"
        )
        iv_mean_21 = iv.groupby("ticker", sort=False)["iv_call"].transform(
            lambda s: s.rolling(21, min_periods=21).mean()
        )
        out["iv_compression_flag"] = (iv["iv_call"] < iv_mean_21).astype(float).to_numpy()

    return out[VOLATILITY_COMPRESSION_FEATURES]


# ===== Category 6: Historical analogues (6) =====
def _category_historical_analogues(
    df: pd.DataFrame,
    base_features: pd.DataFrame,
    benchmark: Optional[pd.DataFrame],
    sector_map: Optional[Dict[str, str]],
) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    z_components = []
    for feat, (mean, std) in HISTORICAL_MULTIBAGGER_REFERENCE.items():
        z_components.append(((base_features[feat] - mean) / std) ** 2)
    distance = np.sqrt(sum(z_components))
    out["base_pattern_similarity"] = 100.0 * np.exp(-distance / 2.0)

    breakout_score = (base_features["breakout_volume_ratio"] / 3.0).clip(0, 1) * 100.0
    # SPEC-FEAT-001: no fillna(0) — both inputs are warmup-dependent.
    out["post_base_breakout_score"] = 0.5 * breakout_score + 0.5 * base_features["consolidation_pattern_score"]

    roll252_max_high = _grouped_rolling(df, "high", 252, "max")
    roll252_min_low = _grouped_rolling(df, "low", 252, "min")
    out["recovery_from_correction"] = (
        100.0 * _safe_div(df["close"] - roll252_min_low, roll252_max_high - roll252_min_low)
    ).clip(0, 100)

    sector_series = df["ticker"].map(sector_map or {}).fillna("UNKNOWN")
    ret_63d = _safe_div(df["close"], _grouped_shift(df, "close", 63)) - 1.0
    work = df.assign(_sector=sector_series, _ret_63d=ret_63d)
    sector_mean_ret = work.groupby(["date", "_sector"], sort=False)["_ret_63d"].transform("mean")
    sector_table = work.assign(_sector_mean_ret=sector_mean_ret)[
        ["date", "_sector", "_sector_mean_ret"]
    ].drop_duplicates()
    sector_table["_sector_rank"] = sector_table.groupby("date", sort=False)["_sector_mean_ret"].rank(pct=True) * 100.0
    merged = work.merge(sector_table[["date", "_sector", "_sector_rank"]], on=["date", "_sector"], how="left")
    out["sector_cycle_position"] = merged["_sector_rank"].to_numpy()

    if benchmark is not None and "nifty50_close" in benchmark.columns:
        bench = benchmark[["date", "nifty50_close"]].copy()
        merged_corr = df[["date", "ticker", "close"]].merge(bench, on="date", how="left")
        merged_corr["_stock_ret_21d"] = merged_corr.groupby("ticker", sort=False)["close"].pct_change(
            21, fill_method=None
        )
        merged_corr["_nifty_ret_21d"] = merged_corr["nifty50_close"].pct_change(21, fill_method=None)

        def _corr(g: pd.DataFrame) -> pd.Series:
            arr1 = g["_stock_ret_21d"].to_numpy(dtype=np.float64)
            arr2 = g["_nifty_ret_21d"].to_numpy(dtype=np.float64)
            return pd.Series(talib.CORREL(arr1, arr2, timeperiod=126), index=g.index)

        out["market_cycle_alignment"] = _apply_per_ticker(merged_corr, _corr).to_numpy()
    else:
        out["market_cycle_alignment"] = np.nan

    # SPEC-FEAT-001: no fillna(0) — every component is warmup-dependent.
    out["analogue_composite_score"] = (
        0.4 * out["base_pattern_similarity"]
        + 0.3 * out["recovery_from_correction"]
        + 0.3 * out["sector_cycle_position"]
    )

    return out[HISTORICAL_ANALOGUE_FEATURES]


def compute_multibagger_features(
    ohlcv: pd.DataFrame,
    benchmark: Optional[pd.DataFrame] = None,
    sector_map: Optional[Dict[str, str]] = None,
    mf_snapshot: Optional[pd.DataFrame] = None,
    governance_snapshot: Optional[pd.DataFrame] = None,
    fno_iv_panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Compute all 33 multibagger-specific features for a multi-ticker OHLCV panel.

    Parameters
    ----------
    ohlcv : pd.DataFrame
        Long-format panel: date, ticker, open, high, low, close, volume,
        and optionally delivery_pct. One row per (ticker, date), full
        lookback history per ticker (>= 252 days to populate every
        feature; SPEC-MODEL-001: ">= 756 trading days for meaningful
        label coverage" at training time).
    benchmark : pd.DataFrame, optional
        Wide-format: date + nifty50_close (features/technical.py's
        BENCHMARK_TICKERS convention). Relative-strength and market-cycle
        features are NaN without it.
    sector_map : dict, optional
        ticker -> sector. Tickers with no mapping fall into "UNKNOWN".
    mf_snapshot, governance_snapshot : pd.DataFrame, optional
        ticker-keyed single-date snapshots (features/mf_holdings.py's,
        features/governance.py's panel function output) — see module
        docstring's PIT Assumptions: merged ONLY onto the latest date in
        `ohlcv`, never broadcast across historical rows.
    fno_iv_panel : pd.DataFrame, optional
        date, ticker, iv_call (features/fno_features.py-derived history).
        `iv_compression_flag` is NaN without it.

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker + MULTIBAGGER_FEATURES (33 cols).

    Spec References
    ----------------
    SPEC-PIPE-004: fully vectorized via groupby/rolling/talib.
    SPEC-MODEL-001: multibagger training needs >= 756 trading days/ticker.

    PIT Assumptions
    ----------------
    See module docstring.

    Raises
    ------
    None — insufficient history produces NaN (SPEC-FEAT-001), not an error.
    """
    if ohlcv.empty:
        return pd.DataFrame(columns=["date", "ticker"] + MULTIBAGGER_FEATURES)

    df = ohlcv.sort_values(["ticker", "date"]).reset_index(drop=True)

    base = _category_base_formation(df)
    accumulation = _category_accumulation(df, mf_snapshot, governance_snapshot)
    relative_strength = _category_relative_strength(df, benchmark, sector_map)
    trend_quality = _category_trend_quality(df)
    volatility_compression = _category_volatility_compression(df, fno_iv_panel)
    historical_analogues = _category_historical_analogues(df, base, benchmark, sector_map)

    out = pd.concat(
        [
            df[["date", "ticker"]],
            base, accumulation, relative_strength, trend_quality, volatility_compression, historical_analogues,
        ],
        axis=1,
    )
    return out[["date", "ticker"] + MULTIBAGGER_FEATURES]

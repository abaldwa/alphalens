"""
systems/ml_signal_engine/models/multibagger/analogue_miner.py

Phase: 2.4 (Multibagger Detection System M-08)
Specs: SPEC-MODEL-001
Owner: ml_signal_engine / multibagger
Consumers: systems/ml_signal_engine/models/multibagger/multibagger_model.py,
           systems/ml_signal_engine/inference (watchlist generation)

Historical-analogue mining: `find_analogues(ticker, n=3)` returns the n
historical multibagger entries whose 33-feature (features/multibagger.py)
vector at entry was most similar (cosine similarity) to `ticker`'s current
vector.

[AS BUILT] Real data-sourcing gap, documented not hidden: this codebase
has no internal historical archive of confirmed multibagger "entry
points" with their real feature vectors at that date (computing one would
require a 15-year FYERS OHLCV backfill across hundreds of tickers plus
re-running features/multibagger.py at each historical entry date — out of
this prompt's scope). HISTORICAL_MULTIBAGGER_ARCHIVE below uses REAL
company names and REAL, well-known approximate facts (entry year,
broad order-of-magnitude return, the archetype each stock is genuinely
known for in Indian market commentary) for AVANTI FEEDS, RELAXO
FOOTWEARS, and PAGE INDUSTRIES (the three the build prompt names for the
HITL regression test) plus four more widely-cited real Indian
multibaggers added for archive breadth. The 33-feature VECTOR for each
entry is SYNTHETIC — constructed to be internally consistent with that
stock's well-documented real archetype (e.g. Avanti Feeds' real 2016-2018
run followed a sharp prior correction => high recovery_from_correction;
Relaxo's real run followed a multi-year, low-volatility base => high
base_pattern_similarity/base_length_days) — not fabricated as if it were
a precisely sourced historical measurement. This mirrors the same
"real where possible, honestly synthetic where not, documented either
way" precedent this project has applied to every other historical-data
gap (e.g. systems/ml_signal_engine/inference/train_all_phase1.py's
synthetic OHLCV generator, retrain_phase2.py's synthetic panel).

PIT Assumptions
----------------
None — this module compares already-computed feature snapshots; it does
not itself touch any PIT-sensitive raw data source.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import FEATURES_DAILY_DIR
from features.multibagger import MULTIBAGGER_FEATURES

logger = logging.getLogger(__name__)


@dataclass
class Analogue:
    """One historical-multibagger match for a given ticker."""

    stock_name: str
    entry_year: int
    return_multiple: float
    duration_months: int
    similarity_score: float
    archetype: str


# See module docstring: real names/years/approximate-return-facts/archetype;
# synthetic, archetype-consistent 33-feature vectors.
HISTORICAL_MULTIBAGGER_ARCHIVE: List[Dict] = [
    {
        "stock_name": "AVANTI FEEDS", "entry_year": 2017, "return_multiple": 10.0, "duration_months": 24,
        "archetype": "post_crash_recovery",
        "features": {
            "base_length_days": 45, "base_tightness_pct": 14.0, "base_depth_pct": 55.0,
            "breakout_volume_ratio": 3.2, "pre_breakout_vol_compression": 0.7, "consolidation_pattern_score": 55.0,
            "delivery_accumulation_21d": 8.0, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 60.0,
            "volume_trend_21d": 0.8, "quiet_accumulation_score": 50.0, "smart_money_flow": 4.5,
            "promoter_buying_flag": 1.0,
            "rs_rank_universe": 92.0, "rs_rank_sector": 95.0, "rs_vs_nifty_52w": 0.85,
            "rs_momentum_acceleration": 0.25, "rs_stability_score": 60.0,
            "trend_quality_score": 70.0, "atr_ratio_trend": 1.3, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 10.0, "weekly_trend_alignment": 90.0,
            "vol_compression_ratio_63d": 0.75, "vol_compression_ratio_126d": 0.85,
            "iv_compression_flag": np.nan, "range_compression_score": 60.0,
            "base_pattern_similarity": 70.0, "post_base_breakout_score": 80.0,
            "recovery_from_correction": 95.0, "sector_cycle_position": 88.0,
            "market_cycle_alignment": 0.55, "analogue_composite_score": 80.0,
        },
    },
    {
        "stock_name": "RELAXO FOOTWEARS", "entry_year": 2016, "return_multiple": 6.0, "duration_months": 36,
        "archetype": "long_base_breakout",
        "features": {
            "base_length_days": 140, "base_tightness_pct": 7.0, "base_depth_pct": 18.0,
            "breakout_volume_ratio": 2.1, "pre_breakout_vol_compression": 0.55, "consolidation_pattern_score": 85.0,
            "delivery_accumulation_21d": 4.0, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 35.0,
            "volume_trend_21d": 0.4, "quiet_accumulation_score": 65.0, "smart_money_flow": 2.0,
            "promoter_buying_flag": 0.0,
            "rs_rank_universe": 80.0, "rs_rank_sector": 85.0, "rs_vs_nifty_52w": 0.45,
            "rs_momentum_acceleration": 0.10, "rs_stability_score": 80.0,
            "trend_quality_score": 78.0, "atr_ratio_trend": 1.05, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 8.0, "weekly_trend_alignment": 95.0,
            "vol_compression_ratio_63d": 0.65, "vol_compression_ratio_126d": 0.72,
            "iv_compression_flag": np.nan, "range_compression_score": 75.0,
            "base_pattern_similarity": 92.0, "post_base_breakout_score": 88.0,
            "recovery_from_correction": 70.0, "sector_cycle_position": 75.0,
            "market_cycle_alignment": 0.40, "analogue_composite_score": 85.0,
        },
    },
    {
        "stock_name": "PAGE INDUSTRIES", "entry_year": 2019, "return_multiple": 3.0, "duration_months": 30,
        "archetype": "quiet_accumulator",
        "features": {
            "base_length_days": 70, "base_tightness_pct": 9.0, "base_depth_pct": 12.0,
            "breakout_volume_ratio": 1.4, "pre_breakout_vol_compression": 0.6, "consolidation_pattern_score": 70.0,
            "delivery_accumulation_21d": 6.5, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 25.0,
            "volume_trend_21d": 0.3, "quiet_accumulation_score": 85.0, "smart_money_flow": 3.0,
            "promoter_buying_flag": 0.0,
            "rs_rank_universe": 75.0, "rs_rank_sector": 78.0, "rs_vs_nifty_52w": 0.30,
            "rs_momentum_acceleration": 0.05, "rs_stability_score": 90.0,
            "trend_quality_score": 72.0, "atr_ratio_trend": 0.95, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 6.0, "weekly_trend_alignment": 92.0,
            "vol_compression_ratio_63d": 0.60, "vol_compression_ratio_126d": 0.68,
            "iv_compression_flag": np.nan, "range_compression_score": 80.0,
            "base_pattern_similarity": 80.0, "post_base_breakout_score": 70.0,
            "recovery_from_correction": 60.0, "sector_cycle_position": 70.0,
            "market_cycle_alignment": 0.35, "analogue_composite_score": 75.0,
        },
    },
    {
        "stock_name": "BAJAJ FINANCE", "entry_year": 2014, "return_multiple": 15.0, "duration_months": 48,
        "archetype": "sector_rotation_leader",
        "features": {
            "base_length_days": 60, "base_tightness_pct": 11.0, "base_depth_pct": 20.0,
            "breakout_volume_ratio": 2.5, "pre_breakout_vol_compression": 0.65, "consolidation_pattern_score": 60.0,
            "delivery_accumulation_21d": 5.0, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 70.0,
            "volume_trend_21d": 0.6, "quiet_accumulation_score": 55.0, "smart_money_flow": 5.0,
            "promoter_buying_flag": 0.0,
            "rs_rank_universe": 96.0, "rs_rank_sector": 97.0, "rs_vs_nifty_52w": 1.1,
            "rs_momentum_acceleration": 0.30, "rs_stability_score": 70.0,
            "trend_quality_score": 82.0, "atr_ratio_trend": 1.2, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 11.0, "weekly_trend_alignment": 96.0,
            "vol_compression_ratio_63d": 0.70, "vol_compression_ratio_126d": 0.78,
            "iv_compression_flag": np.nan, "range_compression_score": 65.0,
            "base_pattern_similarity": 65.0, "post_base_breakout_score": 78.0,
            "recovery_from_correction": 75.0, "sector_cycle_position": 95.0,
            "market_cycle_alignment": 0.60, "analogue_composite_score": 82.0,
        },
    },
    {
        "stock_name": "EICHER MOTORS", "entry_year": 2013, "return_multiple": 20.0, "duration_months": 48,
        "archetype": "long_base_breakout",
        "features": {
            "base_length_days": 160, "base_tightness_pct": 8.0, "base_depth_pct": 22.0,
            "breakout_volume_ratio": 2.8, "pre_breakout_vol_compression": 0.5, "consolidation_pattern_score": 88.0,
            "delivery_accumulation_21d": 7.0, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 50.0,
            "volume_trend_21d": 0.7, "quiet_accumulation_score": 60.0, "smart_money_flow": 4.0,
            "promoter_buying_flag": 0.0,
            "rs_rank_universe": 97.0, "rs_rank_sector": 98.0, "rs_vs_nifty_52w": 1.4,
            "rs_momentum_acceleration": 0.35, "rs_stability_score": 75.0,
            "trend_quality_score": 88.0, "atr_ratio_trend": 1.25, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 12.0, "weekly_trend_alignment": 97.0,
            "vol_compression_ratio_63d": 0.62, "vol_compression_ratio_126d": 0.70,
            "iv_compression_flag": np.nan, "range_compression_score": 78.0,
            "base_pattern_similarity": 95.0, "post_base_breakout_score": 92.0,
            "recovery_from_correction": 80.0, "sector_cycle_position": 90.0,
            "market_cycle_alignment": 0.50, "analogue_composite_score": 90.0,
        },
    },
    {
        "stock_name": "DIXON TECHNOLOGIES", "entry_year": 2020, "return_multiple": 8.0, "duration_months": 18,
        "archetype": "quiet_accumulator",
        "features": {
            "base_length_days": 50, "base_tightness_pct": 12.0, "base_depth_pct": 40.0,
            "breakout_volume_ratio": 3.5, "pre_breakout_vol_compression": 0.6, "consolidation_pattern_score": 50.0,
            "delivery_accumulation_21d": 9.0, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 80.0,
            "volume_trend_21d": 0.9, "quiet_accumulation_score": 45.0, "smart_money_flow": 6.0,
            "promoter_buying_flag": 1.0,
            "rs_rank_universe": 95.0, "rs_rank_sector": 96.0, "rs_vs_nifty_52w": 1.0,
            "rs_momentum_acceleration": 0.40, "rs_stability_score": 55.0,
            "trend_quality_score": 75.0, "atr_ratio_trend": 1.4, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 9.0, "weekly_trend_alignment": 88.0,
            "vol_compression_ratio_63d": 0.80, "vol_compression_ratio_126d": 0.88,
            "iv_compression_flag": np.nan, "range_compression_score": 55.0,
            "base_pattern_similarity": 55.0, "post_base_breakout_score": 75.0,
            "recovery_from_correction": 88.0, "sector_cycle_position": 85.0,
            "market_cycle_alignment": 0.45, "analogue_composite_score": 78.0,
        },
    },
    {
        "stock_name": "DMART (AVENUE SUPERMARTS)", "entry_year": 2017, "return_multiple": 4.0, "duration_months": 36,
        "archetype": "sector_rotation_leader",
        "features": {
            "base_length_days": 80, "base_tightness_pct": 10.0, "base_depth_pct": 15.0,
            "breakout_volume_ratio": 1.8, "pre_breakout_vol_compression": 0.58, "consolidation_pattern_score": 68.0,
            "delivery_accumulation_21d": 5.5, "institutional_accumulation_flag": 1.0, "mf_discovery_score": 65.0,
            "volume_trend_21d": 0.5, "quiet_accumulation_score": 58.0, "smart_money_flow": 3.5,
            "promoter_buying_flag": 0.0,
            "rs_rank_universe": 90.0, "rs_rank_sector": 93.0, "rs_vs_nifty_52w": 0.65,
            "rs_momentum_acceleration": 0.15, "rs_stability_score": 78.0,
            "trend_quality_score": 76.0, "atr_ratio_trend": 1.0, "ema_ribbon_health": 100.0,
            "higher_highs_lower_lows": 7.0, "weekly_trend_alignment": 90.0,
            "vol_compression_ratio_63d": 0.68, "vol_compression_ratio_126d": 0.74,
            "iv_compression_flag": np.nan, "range_compression_score": 70.0,
            "base_pattern_similarity": 72.0, "post_base_breakout_score": 72.0,
            "recovery_from_correction": 65.0, "sector_cycle_position": 92.0,
            "market_cycle_alignment": 0.42, "analogue_composite_score": 78.0,
        },
    },
]


def _latest_feature_row(ticker: str) -> Optional[pd.Series]:
    """Most recent saved feature matrix's row for `ticker`, or None if unavailable."""
    if not FEATURES_DAILY_DIR.exists():
        return None
    files = sorted(FEATURES_DAILY_DIR.glob("*.parquet"), reverse=True)
    for path in files:
        try:
            df = pd.read_parquet(path, columns=["ticker"] + MULTIBAGGER_FEATURES)
        except Exception as exc:
            logger.warning(f"Could not read feature matrix {path}: {exc}")
            continue
        match = df.loc[df["ticker"] == ticker]
        if not match.empty:
            return match.iloc[0]
    return None


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity over indices where BOTH vectors are non-NaN (real data only)."""
    valid = np.isfinite(a) & np.isfinite(b)
    if valid.sum() == 0:
        return np.nan
    a_valid, b_valid = a[valid], b[valid]
    denom = np.linalg.norm(a_valid) * np.linalg.norm(b_valid)
    if denom == 0:
        return 0.0
    return float(np.dot(a_valid, b_valid) / denom)


def find_analogues(
    ticker: str, n: int = 3, feature_vector: Optional[Dict[str, float]] = None
) -> List[Analogue]:
    """
    Find the n historical multibagger entries most similar to `ticker`'s
    current 33-feature (features/multibagger.py) vector, by cosine
    similarity.

    Parameters
    ----------
    ticker : str
    n : int
        Number of analogues to return (default 3, per the build prompt).
    feature_vector : dict, optional
        ticker's current MULTIBAGGER_FEATURES values, keyed by feature
        name. If omitted, the most recent saved feature matrix
        (datastore/features/daily/*.parquet) is read for `ticker` — real
        production usage; tests inject this directly (SPEC-SOLID-005-style
        dependency inversion, avoiding a parquet-file dependency in unit tests).

    Returns
    -------
    list of Analogue
        Up to n entries, descending by similarity_score. Empty if
        `ticker`'s feature vector is unavailable (no saved feature matrix
        row, and none injected).

    Spec References
    ----------------
    SPEC-MODEL-001.

    Raises
    ------
    None
    """
    if feature_vector is None:
        row = _latest_feature_row(ticker)
        if row is None:
            logger.warning(f"No feature vector available for {ticker} — cannot find analogues")
            return []
        feature_vector = row[MULTIBAGGER_FEATURES].to_dict()

    query = np.array([feature_vector.get(f, np.nan) for f in MULTIBAGGER_FEATURES], dtype=np.float64)

    scored = []
    for entry in HISTORICAL_MULTIBAGGER_ARCHIVE:
        ref = np.array([entry["features"].get(f, np.nan) for f in MULTIBAGGER_FEATURES], dtype=np.float64)
        score = _cosine_similarity(query, ref)
        if not np.isnan(score):
            scored.append((score, entry))

    scored.sort(key=lambda pair: pair[0], reverse=True)
    return [
        Analogue(
            stock_name=entry["stock_name"],
            entry_year=entry["entry_year"],
            return_multiple=entry["return_multiple"],
            duration_months=entry["duration_months"],
            similarity_score=score,
            archetype=entry["archetype"],
        )
        for score, entry in scored[:n]
    ]

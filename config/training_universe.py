"""
config/training_universe.py

Phase: ML24 (2026-07-11 model-quality fix batch)
Owner: ml_signal_engine
Consumers: systems/ml_signal_engine/inference/retrain_phase2.py (training set),
           systems/ml_signal_engine/inference/daily_inference.py (in_training_universe
           tagging), datastore/api/routers/* (recommendation-surfacing gate)

Two separate, deliberately different liquidity floors (root cause: ML24/ML27/ML31 —
signal_63d, signal_21d and MultiBagger were all found to be miscalibrated in part
because training data included thin, noisy names never representative of what the
product should recommend):

1. TRAINING universe (`build_training_universe`): ADTV >= TRAINING_ADTV_FLOOR_CR
   (Rs 40cr/day), capped at TRAINING_UNIVERSE_MAX_SIZE (750). Whichever binds first
   wins — as of 2026-07-11 only 530 tickers clear Rs 40cr, so the floor binds, not
   the cap. Refreshed weekly with a hysteresis band (SPEC: a ticker already in last
   week's list stays if it's still in this week's *loosened* pool, at
   HYSTERESIS_ADTV_FLOOR_CR, before falling out) so the list doesn't churn every
   week from tickers oscillating right at the boundary. Models are TRAINED only on
   this set.

2. RECOMMENDATION floor (`is_recommendable`/`filter_recommendable`): ADTV >=
   RECOMMENDATION_ADTV_FLOOR_CR (Rs 20cr/day) — deliberately kept close to the
   training floor (2026-07-11: narrowed from an initial Rs 5cr, which was an 8x
   extrapolation gap from the Rs 40cr training floor — too far outside what the
   model actually learned from). Applies to what's SURFACED to users (watchlists,
   screeners, paper-trading candidates), not to what's trained on or scored. Every
   model still scores the full universe (these are pooled panel models, not
   per-ticker artifacts — a ticker never in the training set can still be scored,
   just out-of-distribution); this floor only gates the presentation layer.
   MultiBagger is the one exception (per product decision, 2026-07-11): sub-floor
   tickers aren't excluded, just shown under a separate "Stocks with ADTV < 20Cr"
   heading rather than the main list.
"""

from __future__ import annotations

import json
import logging
from datetime import date as date_type
from pathlib import Path
from typing import List, Optional

import pandas as pd

from config.settings import TRAINING_UNIVERSE_DIR
from config.universe import load_universe_raw

logger = logging.getLogger(__name__)

TRAINING_ADTV_FLOOR_CR = 40.0  # confirmed 2026-07-11: 530 tickers clear this floor
TRAINING_UNIVERSE_MAX_SIZE = 750
HYSTERESIS_ADTV_FLOOR_CR = 32.0  # looser re-entry bar for names already in last week's list
RECOMMENDATION_ADTV_FLOOR_CR = 20.0  # narrowed 2026-07-11 from 5cr: keep the training/
# recommendation gap tight so "recommended" always means reasonably close to what the
# model actually learned from (training floor is 40cr) rather than an 8x extrapolation

_LIST_VERSION_FORMAT = "%Y%m%d"


def _list_path(as_of: date_type) -> Path:
    return TRAINING_UNIVERSE_DIR / f"training_universe_v{as_of.strftime(_LIST_VERSION_FORMAT)}.json"


def _latest_saved_list() -> Optional[dict]:
    """Most recently saved training-universe snapshot, or None if none exist yet."""
    if not TRAINING_UNIVERSE_DIR.exists():
        return None
    candidates = sorted(TRAINING_UNIVERSE_DIR.glob("training_universe_v*.json"))
    if not candidates:
        return None
    return json.loads(candidates[-1].read_text())


def build_training_universe(
    as_of: Optional[date_type] = None,
    adtv_floor_cr: float = TRAINING_ADTV_FLOOR_CR,
    max_size: int = TRAINING_UNIVERSE_MAX_SIZE,
    hysteresis_floor_cr: float = HYSTERESIS_ADTV_FLOOR_CR,
    universe: Optional[pd.DataFrame] = None,
) -> List[str]:
    """
    Build (but do not save) this week's ADTV-ranked training-universe ticker list.

    Rule: rank all tickers by adtv_cr descending. A ticker qualifies if
    adtv_cr >= adtv_floor_cr, OR (it was in the previously-saved list AND its
    adtv_cr >= hysteresis_floor_cr) — the hysteresis band keeps a ticker
    hovering just below the floor from flapping in and out week to week.
    Then cap at max_size (still ADTV-ranked, so if more than max_size qualify,
    only the top max_size by ADTV are kept).

    Parameters
    ----------
    as_of : date, optional
        Defaults to today. Only used for logging/context, not point-in-time
        ADTV (config.universe.load_universe_raw() always reflects current data —
        see module docstring's point-in-time caveat, ML24 2026-07-11).
    universe : DataFrame, optional
        Injected for testability; defaults to config.universe.load_universe_raw().

    Returns
    -------
    list of str
        Ticker symbols, ADTV-descending, length <= max_size.
    """
    as_of = as_of or date_type.today()
    df = universe if universe is not None else load_universe_raw()

    prev = _latest_saved_list()
    prev_tickers = set(prev["tickers"]) if prev else set()

    qualifies = df["adtv_cr"] >= adtv_floor_cr
    if prev_tickers:
        hysteresis_ok = df["ticker"].isin(prev_tickers) & (df["adtv_cr"] >= hysteresis_floor_cr)
        qualifies = qualifies | hysteresis_ok

    pool = df.loc[qualifies].sort_values("adtv_cr", ascending=False)
    selected = pool.head(max_size)["ticker"].tolist()

    logger.info(
        "build_training_universe(%s): %d tickers qualify (floor=%.1fcr, hysteresis=%.1fcr), "
        "%d kept after max_size=%d cap",
        as_of.isoformat(), int(qualifies.sum()), adtv_floor_cr, hysteresis_floor_cr,
        len(selected), max_size,
    )
    return selected


def save_training_universe(tickers: List[str], as_of: Optional[date_type] = None) -> Path:
    """Persist a training-universe snapshot as a dated JSON (SPEC: versioned, auditable list)."""
    as_of = as_of or date_type.today()
    TRAINING_UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    path = _list_path(as_of)
    path.write_text(json.dumps({
        "as_of": as_of.isoformat(),
        "adtv_floor_cr": TRAINING_ADTV_FLOOR_CR,
        "max_size": TRAINING_UNIVERSE_MAX_SIZE,
        "tickers": tickers,
        "count": len(tickers),
    }, indent=2))
    logger.info("Saved training universe (%d tickers) -> %s", len(tickers), path)
    return path


def refresh_training_universe(as_of: Optional[date_type] = None) -> List[str]:
    """Weekly job entry point: build + save this week's training-universe list."""
    tickers = build_training_universe(as_of=as_of)
    save_training_universe(tickers, as_of=as_of)
    return tickers


def load_current_training_universe() -> List[str]:
    """
    Load the most recently saved training-universe list. Builds and saves a
    fresh one on first-ever call (no prior snapshot) rather than returning
    an empty list.
    """
    saved = _latest_saved_list()
    if saved is None:
        logger.warning("No training-universe snapshot found — building one now (first run).")
        return refresh_training_universe()
    return saved["tickers"]


def is_recommendable(adtv_cr: float, floor_cr: float = RECOMMENDATION_ADTV_FLOOR_CR) -> bool:
    """True if a single ticker's ADTV clears the (much looser) recommendation floor."""
    return adtv_cr is not None and adtv_cr >= floor_cr


def filter_recommendable(
    df: pd.DataFrame, ticker_col: str = "ticker", floor_cr: float = RECOMMENDATION_ADTV_FLOOR_CR,
    universe: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Drop rows whose ticker's ADTV is below the recommendation floor (Rs 5cr/day
    default). Used at the presentation layer (watchlists, screeners, paper-trading
    candidates) — NOT at scoring time, every ticker is still scored/tagged
    regardless (ML24 2026-07-11: MultiBagger is the one caller that does NOT use
    this — it shows sub-floor tickers under a separate heading instead of dropping
    them).
    """
    ref = universe if universe is not None else load_universe_raw()
    adtv_map = dict(zip(ref["ticker"], ref["adtv_cr"]))
    keep = df[ticker_col].map(lambda t: is_recommendable(adtv_map.get(t), floor_cr))
    return df.loc[keep].reset_index(drop=True)

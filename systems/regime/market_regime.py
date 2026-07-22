"""
systems/regime/market_regime.py

Rule-based Bull/Bear/Sideways market-phase classifier, producing
contiguous DATE-RANGE segments (not daily point labels) from an index's
daily close price — the "20% threshold" convention most bull/bear market
commentary uses (S&P Dow Jones Indices, Yardeni Research, etc.), extended
with an explicit Sideways state for consolidation stretches that never
confirm a 20% move in either direction within a bounded window.

There is no single universal definition of Bull/Bear/Sideways markets —
this module picks one specific, deterministic, explainable rule rather
than the existing HMM-based systems/ml_signal_engine/models/hmm/
regime_detector.py, which classifies daily probability-weighted states
from a different input set (returns/volatility/volume/ATR) and is not
segment-based. The two are deliberately separate and namespaced
(market_regimes table vs ml_signals.hmm_regime / GET /api/v1/macro/
regime) — see datastore/schema/create_normalised.py's
_CREATE_MARKET_REGIMES comment.

Algorithm ("20% threshold + consolidation timeout"):
  Walk the price series chronologically, tracking the running peak/trough
  since the start of the CURRENT (unconfirmed) segment.
    - BULL confirms once price has rallied >= BULL_BEAR_THRESHOLD_PCT from
      the running trough -> a new segment starts AT THE TROUGH's date
      (backdated), CONFIRMED on the day the threshold was actually crossed.
    - BEAR confirms once price has fallen >= BULL_BEAR_THRESHOLD_PCT from
      the running peak -> new segment starts at the peak's date.
    - SIDEWAYS: if neither move confirms within SIDEWAYS_WINDOW_TRADING_DAYS
      of the current segment's start, the stretch is closed out as
      SIDEWAYS at that window boundary and a fresh window starts there —
      splitting a long consolidation into successive Sideways segments
      rather than one arbitrarily long blob.
  This yields a fully-covering, non-overlapping sequence of segments; the
  final segment is always "open" (its end may be revised by future data).

PIT-safety: start_date is backdated to the actual peak/trough day, but
confirmed_date is the day the threshold rule actually fired — always
>= start_date. A caller doing point-in-time analysis must gate on
confirmed_date, not start_date, or it will look ahead.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Literal, Optional

import pandas as pd

logger = logging.getLogger(__name__)

Regime = Literal["bull", "bear", "sideways"]

BULL_BEAR_THRESHOLD_PCT = 0.20
SIDEWAYS_WINDOW_TRADING_DAYS = 126  # ~6 months
METHOD_NAME = "20pct_threshold_v1"


@dataclass(frozen=True)
class RegimeSegment:
    regime: Regime
    start_date: date
    end_date: date
    confirmed_date: date
    move_pct: Optional[float]  # % move from the anchor that confirmed this segment; None while still open/unconfirmed


def classify_regimes(prices: pd.Series) -> List[RegimeSegment]:
    """
    prices: a pandas Series of close prices indexed by date (ascending,
    trading-day index — gaps for holidays/weekends are fine), for ONE
    index. NaNs are dropped before classification.

    Returns contiguous, non-overlapping RegimeSegment objects covering
    prices.index[0] through prices.index[-1]. The LAST segment is always
    "open" — it may still be extended or reclassified once more data
    arrives, so treat it as provisional, not a confirmed historical fact.
    """
    prices = prices.dropna()
    if prices.empty:
        return []

    dates = list(prices.index)
    values = list(prices.astype(float).values)
    n = len(values)

    segments: List[RegimeSegment] = []
    regime: Regime = "sideways"
    segment_start = 0
    peak_idx = 0
    trough_idx = 0

    def emit(end_idx: int, confirmed_idx: int, new_start_idx: int) -> None:
        # A segment's own move_pct is always measured from ITS start (a
        # bull segment starts at its trough, a bear segment at its peak,
        # by construction) to its end — not from whatever running
        # peak/trough tracker happens to hold at close time, which drifts
        # for unrelated future-detection purposes once the segment is over.
        nonlocal segment_start
        if end_idx >= segment_start:
            anchor = values[segment_start]
            move_pct = (values[end_idx] - anchor) / anchor if anchor else None
            segments.append(
                RegimeSegment(
                    regime=regime,
                    start_date=_to_date(dates[segment_start]),
                    end_date=_to_date(dates[end_idx]),
                    confirmed_date=_to_date(dates[confirmed_idx]),
                    move_pct=move_pct,
                )
            )
        segment_start = new_start_idx

    i = 1
    while i < n:
        if values[i] > values[peak_idx]:
            peak_idx = i
        if values[i] < values[trough_idx]:
            trough_idx = i

        rally = (values[i] - values[trough_idx]) / values[trough_idx] if values[trough_idx] else 0.0
        drawdown = (values[peak_idx] - values[i]) / values[peak_idx] if values[peak_idx] else 0.0

        if regime != "bull" and rally >= BULL_BEAR_THRESHOLD_PCT:
            emit(trough_idx - 1, i, trough_idx)
            regime = "bull"
            peak_idx = i
            trough_idx = i
        elif regime != "bear" and drawdown >= BULL_BEAR_THRESHOLD_PCT:
            emit(peak_idx - 1, i, peak_idx)
            regime = "bear"
            peak_idx = i
            trough_idx = i
        elif regime == "sideways" and (i - segment_start) >= SIDEWAYS_WINDOW_TRADING_DAYS:
            emit(i, i, i)
            peak_idx = i
            trough_idx = i

        i += 1

    # Final open segment: whatever move has accrued since segment_start,
    # without waiting for it to actually confirm.
    last_idx = n - 1
    anchor = values[segment_start]
    move_pct = (values[last_idx] - anchor) / anchor if anchor else None
    segments.append(
        RegimeSegment(
            regime=regime,
            start_date=_to_date(dates[segment_start]),
            end_date=_to_date(dates[last_idx]),
            confirmed_date=_to_date(dates[last_idx]),
            move_pct=move_pct,
        )
    )
    return segments


def _to_date(d) -> date:
    return d.date() if hasattr(d, "date") else d

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


def method_name(threshold_pct: float) -> str:
    """The `method` string persisted alongside segments produced with this
    threshold — encodes the threshold so different-threshold segments for
    the same index are distinguishable (e.g. "5pct_threshold_v1",
    "20pct_threshold_v1")."""
    return f"{int(round(threshold_pct * 100))}pct_threshold_v1"


def classify_regimes(prices: pd.Series, threshold_pct: float = BULL_BEAR_THRESHOLD_PCT) -> List[RegimeSegment]:
    """
    prices: a pandas Series of close prices indexed by date (ascending,
    trading-day index — gaps for holidays/weekends are fine), for ONE
    index. NaNs are dropped before classification.

    threshold_pct: the Bull/Bear confirmation threshold as a fraction (e.g.
    0.20 for 20%, 0.05 for 5%). Defaults to BULL_BEAR_THRESHOLD_PCT (20%),
    preserving prior behavior for any existing caller that doesn't pass
    this explicitly. Lower thresholds confirm regime flips off smaller
    moves, producing MORE, SHORTER segments than a higher threshold on the
    same price series.

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

        if regime != "bull" and rally >= threshold_pct:
            emit(trough_idx - 1, i, trough_idx)
            regime = "bull"
            peak_idx = i
            trough_idx = i
        elif regime != "bear" and drawdown >= threshold_pct:
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


DRAWDOWN_METHOD_NAME_TEMPLATE = "{pct}pct_drawdown_from_running_peak_v1"


def drawdown_method_name(threshold_pct: float) -> str:
    """Method string for running-peak-drawdown labels, deliberately distinct
    from method_name()'s "{n}pct_threshold_v1" so the two can never be
    confused in market_regimes (they answer different questions)."""
    return DRAWDOWN_METHOD_NAME_TEMPLATE.format(pct=int(round(threshold_pct * 100)))


def bear_by_running_peak_drawdown(
    prices: pd.Series, threshold_pct: float = BULL_BEAR_THRESHOLD_PCT
) -> pd.Series:
    """Point-in-time bear/bull label per date: bear on day t iff the close on
    t is at least `threshold_pct` below the highest close observed UP TO AND
    INCLUDING t. Returns a Series of "bear"/"bull" indexed like `prices`.

    2026-08-09, added for live-deployable regime gating. This is a different
    instrument from classify_regimes() and exists because that one cannot be
    used as a trading gate:

      - classify_regimes() produces SEGMENTS whose start_date is backdated to
        the anchoring peak, and whose confirmed_date (the day the rule could
        actually fire) lags that start by months. On the real Nifty 500 at a
        12% threshold, all four 2021-2026 bear segments confirmed AFTER they
        had already ended. Gating trades on it means either lookahead (match
        on start_date) or a signal that arrives past the trough (match on
        confirmed_date) — useless in both directions.
      - This function has ZERO confirmation lag by construction: it uses a
        running (expanding) max, so the value for day t depends only on data
        through t. The identical rule runs in a backtest and in live trading,
        which is the whole point — the backtest must test what gets deployed.

    Deliberately binary (no "sideways"): as a buy-gate the only question is
    "is the index far enough off its own high to stop buying," and a third
    state would need a second, separately-justified threshold.

    Note this labels the DRAWDOWN state, not a market cycle — the label flips
    back to bull as soon as price recovers to within threshold_pct of the
    running peak, which may be well before a cycle-based classifier would
    call the bear over. That responsiveness is the intent.
    """
    prices = prices.dropna()
    if prices.empty:
        return pd.Series(dtype=object)
    running_peak = prices.astype(float).cummax()
    drawdown = prices.astype(float) / running_peak - 1.0
    return pd.Series(
        ["bear" if d <= -threshold_pct else "bull" for d in drawdown], index=prices.index, dtype=object
    )

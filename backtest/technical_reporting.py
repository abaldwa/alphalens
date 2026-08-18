"""
backtest/technical_reporting.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: scripts/run_technical_experimentation.py,
    scripts/run_technical_recommended_strategies.py,
    backtest/export_trade_book.py

Two report-building helpers specific to backtest.portfolio.Trade objects
(entry_feature_vector already populated by backtest/core/engine.py's
portfolio.buy() call — see that module's 2026-08-01 comment):

  entry_signal_zscores() — how statistically extreme each trade's entry
      template-match score was vs. the same template's own score
      distribution across the run, independent of the return-based outlier
      z-score already covered by backtest.momentum_metrics.
  signal_failure_breakdown() — losing closed trades only, each with its
      entry-condition snapshot, plus winners-vs-losers aggregate stats
      (2026-08-01 user request: "test strategies when the actual signal
      which triggered a buy failed").

Both operate on a plain List[Trade] (or dicts already shaped like one via
dataclasses.asdict) — no DB/file I/O, no orchestrator dependency, so they
can be unit-tested against small synthetic trade lists without running a
real backtest.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backtest.core.metrics import return_population_zscores
from backtest.portfolio import Trade


def _entry_score(trade: Trade) -> Optional[float]:
    fv = trade.entry_feature_vector
    if not fv:
        return None
    score = fv.get("score")
    return float(score) if score is not None else None


def entry_signal_zscores(trades: List[Trade]) -> Dict[str, Any]:
    """Per-trade z-score of entry_feature_vector["score"] (the screener
    template-match strength at buy time) against the population of scores
    across every trade in `trades` — same population-stats primitive the
    return-outlier z-score uses (return_population_zscores), applied to a
    different quantity. A low value flags "this buy was only a marginal/
    weak match" — useful context read next to the trade's actual outcome,
    but computed independently of it (this never looks at pnl/return).

    Returns
    -------
    dict with:
      per_trade  — list of {"ticker", "buy_date", "entry_signal_score",
          "entry_signal_zscore"}, same order/length as `trades`.
      mean_score / std_score — population stats, or None if <3 trades had
          a real score (same small-population guard as the return z-score).
    """
    scores = [_entry_score(t) for t in trades]
    z_result = return_population_zscores(scores)  # not %-returns, but the same z-score math applies to any scalar population
    valid = [s for s in scores if s is not None]
    per_trade = [
        {
            "ticker": t.ticker,
            "buy_date": str(t.entry_date),
            "entry_signal_score": s,
            "entry_signal_zscore": z,
        }
        for t, s, z in zip(trades, scores, z_result["zscores"])
    ]
    return {
        "per_trade": per_trade,
        "mean_score": (sum(valid) / len(valid)) if len(valid) >= 3 else None,
        "n_scored": len(valid),
    }


def signal_failure_breakdown(trades: List[Trade]) -> Dict[str, Any]:
    """Losing CLOSED trades only (pnl_pct < 0), each with its entry-time
    feature_vector snapshot, plus a winners-vs-losers comparison of
    matched_conditions/total_conditions — a template match that only
    barely cleared its own bar (a low ratio) disproportionately explaining
    losses would show up as a lower mean ratio for losers than winners.

    Trades with no entry_feature_vector (e.g. combo-adapter legs where the
    concept doesn't apply, or an adapter that doesn't return matched_conditions)
    are included in the loss list (still real trades a user should be able
    to see) but excluded from the ratio comparison — never fabricated.
    """
    losers = [t for t in trades if t.pnl_pct is not None and t.pnl_pct < 0]
    winners = [t for t in trades if t.pnl_pct is not None and t.pnl_pct >= 0]

    def _ratio(t: Trade) -> Optional[float]:
        fv = t.entry_feature_vector
        if not fv or not fv.get("matched") or not fv.get("total_conditions"):
            return None
        matched = fv.get("matched_conditions")
        total = fv.get("total_conditions")
        if matched is None or not total:
            return None
        # Both come out of an untyped feature-vector dict; the guard above
        # already establishes they are present and total is non-zero.
        ratio: float = matched / total
        return ratio

    loser_ratios = [r for r in (_ratio(t) for t in losers) if r is not None]
    winner_ratios = [r for r in (_ratio(t) for t in winners) if r is not None]

    losing_trades = [
        {
            "ticker": t.ticker,
            "buy_date": str(t.entry_date),
            "sell_date": str(t.exit_date),
            "buy_price": t.entry_price,
            "sell_price": t.exit_price,
            "pnl_pct": t.pnl_pct,
            "exit_reason": t.exit_reason,
            "entry_feature_vector": t.entry_feature_vector,
        }
        for t in losers
    ]

    return {
        "n_losing_trades": len(losers),
        "n_winning_trades": len(winners),
        "losing_trades": losing_trades,
        "mean_matched_conditions_ratio_losers": (
            sum(loser_ratios) / len(loser_ratios) if loser_ratios else None
        ),
        "mean_matched_conditions_ratio_winners": (
            sum(winner_ratios) / len(winner_ratios) if winner_ratios else None
        ),
    }

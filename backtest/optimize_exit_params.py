"""
backtest/optimize_exit_params.py

Phase: 3.x (Technical backtest refactor — exit-regime redesign)
Owner: backtest
Consumers: scripts/derive_exit_params_from_unconstrained.py,
           tests/unit/test_optimize_exit_params.py

Chooses (stop_pct, target_pct, max_hold_days) per template by REPLAYING each
unconstrained trade's daily path under candidate barriers and scoring the
resulting return stream. This supersedes the percentile derivation in
backtest/derive_exit_params.py, which answered "what barriers avoid cutting
winners" — a safety question. The question actually being asked is "what
barriers produce the best return for this strategy", and the two have
different answers: a tighter stop that recycles capital into the next signal
can beat a loose one even while stopping out more eventual winners.

WHY A REPLAY AND NOT A FORMULA
MAE and MFE say a trade reached -12% and +30%, but not WHICH CAME FIRST — and
that single fact decides whether the trade was a stop or a target. There is no
way to recover the ordering from summary statistics, so barriers cannot be
scored without walking the path day by day. Any derivation that skips this is
guessing at the sign of its own result.

INTRABAR AMBIGUITY, RESOLVED PESSIMISTICALLY
When one bar's low breaches the stop AND its high reaches the target, daily
data cannot say which the price touched first. This module always books the
STOP. That is not conservatism for its own sake: the optimistic choice makes
every wide-target/tight-stop combination look free, which is precisely the
parameter region the optimiser searches hardest, so an optimistic tie-break
does not merely flatter the result — it steers the selection.

OVERFITTING IS THE MAIN RISK HERE
This searches a 3-D grid per template against ONE realised path of ~17 years.
The best cell on that path is partly luck, and a naive argmax reliably picks
it. Three defences, all on by default:
  - a drawdown guard rejecting cells materially worse than unconstrained;
  - a minimum-trades floor, since a cell surviving on 20 trades is noise;
  - reporting the runner-up spread, so a cell that wins by 0.1% CAGR over a
    broad plateau is visibly not a real optimum.
None of this makes the result out-of-sample valid. Walk-forward selection is
the actual fix and is a separate step; these only stop the obvious failures.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252

# Candidate grids. Deliberately coarse: a finer grid does not find a better
# strategy, it finds a better fit to this particular path.
DEFAULT_STOP_GRID = (-0.03, -0.05, -0.07, -0.10, -0.15, -0.20, -0.25)
DEFAULT_TARGET_GRID = (0.05, 0.08, 0.10, 0.15, 0.20, 0.30, 0.50)
DEFAULT_MAX_HOLD_GRID = (7, 14, 21, 42, 63, 126, 252)

# A cell must beat this many trades to be eligible.
MIN_TRADES_PER_CELL = 100

# Drawdown guard: reject a cell whose max drawdown is more than this multiple
# of the unconstrained baseline's. Set to 1.25 rather than 1.0 because some
# extra drawdown in exchange for materially higher return is a legitimate
# trade; this rejects only the cells that buy return with disproportionate risk.
MAX_DRAWDOWN_MULTIPLE = 1.25


@dataclass(frozen=True)
class Objective:
    """How a candidate cell is scored. Both supported objectives are built
    because they answer different questions and can disagree — CAGR asks what
    grew the most, risk-adjusted asks what an investor could have held
    through."""

    name: str

    def score(self, returns: np.ndarray, holding_days: np.ndarray) -> float:
        if self.name == "cagr":
            return _cagr(returns, holding_days)
        if self.name == "sharpe":
            return _sharpe(returns)
        if self.name == "calmar":
            dd = _max_drawdown(returns)
            return _cagr(returns, holding_days) / dd if dd > 0 else float("-inf")
        raise ValueError(f"unknown objective {self.name!r}")


CAGR = Objective("cagr")
SHARPE = Objective("sharpe")
CALMAR = Objective("calmar")


def _equity_curve(returns: np.ndarray) -> np.ndarray:
    return np.cumprod(1.0 + returns)


def _cagr(returns: np.ndarray, holding_days: np.ndarray) -> float:
    """Compounded growth per year of CAPITAL DEPLOYMENT, not per calendar year
    of the backtest window.

    Using deployed time is what makes two parameter sets comparable: a cell
    that exits at 7 days and one that exits at 252 days recycle capital at
    completely different rates, and scoring both against the same wall-clock
    window would credit the slow one for time it spent holding rather than
    compounding.
    """
    if len(returns) == 0:
        return float("-inf")
    total = float(np.prod(1.0 + returns))
    if total <= 0:
        return -1.0  # wiped out; a real and rankable outcome, not an error
    years = float(np.sum(holding_days)) / TRADING_DAYS_PER_YEAR
    if years <= 0:
        return float("-inf")
    return total ** (1.0 / years) - 1.0


def _sharpe(returns: np.ndarray) -> float:
    """Per-trade Sharpe. Risk-free is omitted deliberately — these are
    per-trade returns of varying length, so subtracting an annual rate from
    each would penalise long holds twice (once here, once in _cagr's deployed
    -time denominator). Used for RANKING cells against each other, where a
    common constant cancels, never reported as an annualised Sharpe."""
    if len(returns) < 2:
        return float("-inf")
    sd = float(np.std(returns, ddof=1))
    return float(np.mean(returns)) / sd if sd > 0 else float("-inf")


def _max_drawdown(returns: np.ndarray) -> float:
    """Peak-to-trough decline of the trade-sequence equity curve, as a positive
    fraction. This is drawdown in trade order, not in calendar time — it
    understates true drawdown when trades overlap, so it is used only to
    COMPARE cells computed the same way, never quoted as the strategy's
    drawdown."""
    if len(returns) == 0:
        return 0.0
    eq = _equity_curve(returns)
    peak = np.maximum.accumulate(eq)
    return float(np.max((peak - eq) / peak)) if len(eq) else 0.0


def replay_trade(
    path: pd.DataFrame, buy_price: float, stop_pct: float, target_pct: float, max_hold_days: int
) -> Tuple[float, int]:
    """Replays one trade under one set of barriers.

    path : daily bars AFTER the entry bar, ascending, with high/low/close.
    Returns (realised_return_fraction, holding_days).

    A barrier cannot fire on the entry bar itself, which is why the caller
    passes bars strictly after entry: filling a stop on the same bar that
    opened the position implies acting on a price the entry decision already
    consumed.
    """
    if path.empty:
        return 0.0, 0
    stop_price = buy_price * (1.0 + stop_pct)
    target_price = buy_price * (1.0 + target_pct)

    horizon = path.iloc[:max_hold_days]
    for i, (_, bar) in enumerate(horizon.iterrows(), start=1):
        hit_stop = bar["low"] <= stop_price
        hit_target = bar["high"] >= target_price
        # Stop wins ties — see the module docstring on intrabar ambiguity.
        if hit_stop:
            return stop_pct, i
        if hit_target:
            return target_pct, i
    last = horizon.iloc[-1]
    return float(last["close"] / buy_price - 1.0), len(horizon)


def evaluate_cell(
    trades: Sequence[Tuple[pd.DataFrame, float]],
    stop_pct: float,
    target_pct: float,
    max_hold_days: int,
    objective: Objective,
) -> Dict[str, float]:
    results = [replay_trade(p, bp, stop_pct, target_pct, max_hold_days) for p, bp in trades]
    returns = np.array([r for r, _ in results], dtype=float)
    days = np.array([d for _, d in results], dtype=float)
    return {
        "score": objective.score(returns, days),
        "cagr": _cagr(returns, days),
        "sharpe": _sharpe(returns),
        "max_drawdown": _max_drawdown(returns),
        "n_trades": float(len(returns)),
        "win_rate": float((returns > 0).mean() * 100) if len(returns) else float("nan"),
        "avg_hold_days": float(days.mean()) if len(days) else float("nan"),
    }


def optimize(
    trades: Sequence[Tuple[pd.DataFrame, float]],
    objective: Objective = CAGR,
    stop_grid: Iterable[float] = DEFAULT_STOP_GRID,
    target_grid: Iterable[float] = DEFAULT_TARGET_GRID,
    max_hold_grid: Iterable[int] = DEFAULT_MAX_HOLD_GRID,
    baseline_max_drawdown: Optional[float] = None,
    max_drawdown_multiple: float = MAX_DRAWDOWN_MULTIPLE,
    min_trades: int = MIN_TRADES_PER_CELL,
) -> pd.DataFrame:
    """Scores every grid cell and returns them ranked, best first.

    Returns the FULL ranked frame rather than just the winner, because the
    shape of the ranking is the evidence about whether the winner means
    anything: a champion sitting on a broad plateau of near-equal cells is a
    real effect, and one towering over its neighbours is a fit to noise. A
    function that returned only the argmax would hide the distinction.
    """
    rows: List[Dict[str, float]] = []
    for stop in stop_grid:
        for target in target_grid:
            for hold in max_hold_grid:
                m = evaluate_cell(trades, stop, target, hold, objective)
                m.update(stop_pct=stop, target_pct=target, max_hold_days=hold)
                m["eligible"] = m["n_trades"] >= min_trades
                if baseline_max_drawdown is not None and baseline_max_drawdown > 0:
                    m["dd_ratio"] = m["max_drawdown"] / baseline_max_drawdown
                    m["eligible"] = m["eligible"] and m["dd_ratio"] <= max_drawdown_multiple
                rows.append(m)

    frame = pd.DataFrame(rows)
    # Ineligible cells are RETAINED and flagged, not dropped: seeing that the
    # highest-scoring cell was rejected by the drawdown guard is information,
    # and silently removing it would make the guard invisible.
    return frame.sort_values(["eligible", "score"], ascending=[False, False]).reset_index(drop=True)


def plateau_width(ranked: pd.DataFrame, tolerance: float = 0.10) -> int:
    """How many eligible cells score within `tolerance` (relative) of the best.

    A width of 1 means the winner is alone and should be distrusted; a wide
    plateau means the choice is robust to the grid. Reported alongside every
    selected parameter set.
    """
    eligible = ranked[ranked["eligible"]]
    if eligible.empty:
        return 0
    best = eligible["score"].iloc[0]
    if not np.isfinite(best) or best <= 0:
        return int((eligible["score"] >= best).sum())
    return int((eligible["score"] >= best * (1.0 - tolerance)).sum())

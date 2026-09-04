"""
R07: Crash-Aware Momentum Overlay

Same shared ranking as R01/R03/R14-R17 (see common/signals.py::
TrailingMomentumSignal's module note) — the only difference is a
regime-conditional overlay applied AFTER ranking: during a detected
"crash regime" (drawdown + elevated volatility, see
common/crash_regime.py), new buys are disabled and existing holdings are
partially trimmed, rather than riding the full drawdown or missing the
recovery.

Ported from backtest/adapters/momentum_adapter.py's crash_regime_enabled
branch (lines ~892-840, see that file's "[Phase 7]" comments) — faithful
to the legacy list-swap + crash-trim + buy-disable + conviction-reduce
mechanism, with these deliberate departures, all flagged rather than
silently dropped:

1. Crash detection uses ONLY the benchmark_equity path — the legacy
   self-referential fallback (detecting crashes off the strategy's OWN
   P&L when no benchmark was supplied) is not ported. The legacy code
   itself documents benchmark_equity as the preferred mode (Phase 7 fix,
   2026-09-02) since crash regime is a market-wide signal, not a
   basket's own performance.
2. **The benchmark is resolved from the strategy's BAND, not passed in
   per-strategy** (explicit user instruction, 2026-09-04: "Index Equity
   Curve is to be attached to a band and not to a strategy") — via
   common/benchmark.py, the SAME band->index mapping R09's regime
   detection uses. `resolve_universe()`/`resolve_band_universe()`
   already established band-scoped ranking; this extends the same
   principle to band-scoped market context.
3. ADTV-based sizing and circuit-lock handling now PLUGGED IN via
   common/liquidity.py (2026-09-04) — see `_filter_tradeable()` below.

M7 (band_id=7) has no resolved benchmark index yet (see
common/benchmark.py's module docstring) — R07 on band 7 will raise the
first time crash detection actually runs a query; excluded from
R07QueueGenerator.BANDS until that mapping is decided.

Held-position tracking: unlike R01/R03/R14-R17 (which rebuild the target
basket from scratch every rebalance with no memory of prior holdings),
R07 needs to know what it already holds to decide which existing
positions to trim during a crash — so this StrategyAdapter carries
`self._held` state across rebalance() calls, updated at the end of each
call, matching how the legacy adapter tracks it (`self._held`, an
adapter-level attribute, not queried back from the portfolio).
"""

from typing import Any, Dict, List, Optional, Set
import math

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.crash_regime import crash_regime_detector
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R07"
RANK_METHOD = "trailing_return"

# Legacy defaults (backtest/adapters/momentum_adapter.py's crash_* fields)
DEFAULT_DRAWDOWN_THRESHOLD = -0.15
DEFAULT_VOL_PERCENTILE_THRESHOLD = 0.75
DEFAULT_VOL_LOOKBACK_DAYS = 20
DEFAULT_REGIME_LOOKBACK_DAYS = 252
DEFAULT_CRASH_DISABLE_BUYS = True


class R07CrashAware(StrategyBase):
    """Trailing-return momentum with a crash-regime buy-disable + trim overlay."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "Daniel & Moskowitz (2016)-style momentum crash overlay"

    def __init__(
        self,
        band_id: int,
        top_n: int,
        lookback_months: int,
        rebalance_cadence_days: int,
        filter_preset: str = "all_risk",
        crash_disable_buys: bool = DEFAULT_CRASH_DISABLE_BUYS,
        crash_reduce_sizing: Optional[float] = None,
        crash_drawdown_threshold: float = DEFAULT_DRAWDOWN_THRESHOLD,
        crash_vol_percentile_threshold: float = DEFAULT_VOL_PERCENTILE_THRESHOLD,
        crash_vol_lookback_days: int = DEFAULT_VOL_LOOKBACK_DAYS,
        min_adtv_cr: Optional[float] = None,
        **kwargs: Any,
    ):
        super().__init__(
            band_id, top_n, lookback_months, rebalance_cadence_days,
            filter_preset=filter_preset, crash_regime_enabled=True,
            crash_disable_buys=crash_disable_buys,
            crash_reduce_sizing=crash_reduce_sizing,
            crash_drawdown_threshold=crash_drawdown_threshold,
            crash_vol_percentile_threshold=crash_vol_percentile_threshold,
            crash_vol_lookback_days=crash_vol_lookback_days,
            min_adtv_cr=min_adtv_cr,
            **kwargs,
        )
        self.signal = TrailingMomentumSignal(lookback_months=lookback_months)
        self.crash_disable_buys = crash_disable_buys
        self.crash_reduce_sizing = crash_reduce_sizing
        self.crash_drawdown_threshold = crash_drawdown_threshold
        self.crash_vol_percentile_threshold = crash_vol_percentile_threshold
        self.crash_vol_lookback_days = crash_vol_lookback_days
        self.min_adtv_cr = min_adtv_cr
        self._held: Set[str] = set()
        self._benchmark_equity: Optional[pd.Series] = None  # lazily loaded from the band, cached per instance

    def _in_crash_regime(self, as_of_date: str, conn: Any) -> bool:
        """
        Benchmark is resolved from self.band_id (common/benchmark.py) —
        see this file's module docstring for why it's band-attached, not
        passed per-strategy.
        """
        if self._benchmark_equity is None:
            from momentum_framework.common.benchmark import load_benchmark_equity_curve
            self._benchmark_equity = load_benchmark_equity_curve(self.band_id, conn)
        if self._benchmark_equity.empty:
            return False
        try:
            crash_series = crash_regime_detector(
                self._benchmark_equity,
                drawdown_threshold=self.crash_drawdown_threshold,
                vol_percentile_threshold=self.crash_vol_percentile_threshold,
                lookback_days=DEFAULT_REGIME_LOOKBACK_DAYS,
                vol_lookback_days=self.crash_vol_lookback_days,
            )
        except (ValueError, KeyError):
            return False
        ts = pd.Timestamp(as_of_date)
        return bool(crash_series.get(ts, False))

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        target: Set[str] = set(winners.index)

        in_crash = self._in_crash_regime(as_of_date, conn)

        # Plain list-swap: keep whatever's both held and still in target.
        keep = self._held & target

        # [Phase 7 fix] Trim EXISTING holdings during crash, not just gate
        # new buys — else all the crash downside stays in the book. Trim
        # `keep` to its top crash_reduce_sizing fraction by momentum score;
        # the rest fall through to the sell path below like any rotated-out name.
        if in_crash and self.crash_reduce_sizing is not None and keep:
            held_scores = scores.reindex(list(keep)).dropna().sort_values(ascending=False)
            n_keep = math.floor(len(held_scores) * self.crash_reduce_sizing)
            if n_keep < len(held_scores):
                trimmed_out = set(held_scores.index[n_keep:])
                keep -= trimmed_out

        buys_disabled = in_crash and self.crash_disable_buys
        new_entrants = [] if buys_disabled else sorted(target - self._held)

        # Circuit-lock + ADTV filter (common/liquidity.py) — a locked or
        # too-illiquid name is unfillable at this close; skipped THIS
        # rebalance only, not permanently (target is recomputed fresh
        # next call, so it's naturally reconsidered).
        if new_entrants:
            from momentum_framework.common.liquidity import filter_tradeable
            new_entrants = filter_tradeable(conn, new_entrants, as_of_date, min_adtv_cr=self.min_adtv_cr)

        # Emit "buy" for the FULL target (kept + new), not just new
        # entrants — Portfolio.rebalance_to_target() treats the buy set
        # as the complete desired basket for this period and infers sells
        # from absence (see backtesting/portfolio.py's module docstring,
        # "CORRECTNESS FIX 2026-09-04"). A kept ticker re-appearing here
        # is a no-op at execution time (Portfolio leaves ticker∩target
        # untouched, no wash trade) — this is bookkeeping, not a trade.
        final_target = keep | set(new_entrants)
        signals: List[Signal] = []
        for rank, ticker in enumerate(sorted(final_target)):
            conviction = float(scores.get(ticker, 0.0))
            if in_crash and self.crash_reduce_sizing is not None and ticker in new_entrants:
                conviction *= self.crash_reduce_sizing
            signals.append(Signal(ticker=ticker, action="buy", conviction=conviction, rank=rank + 1))

        self._held = final_target
        return signals


class R07QueueGenerator(QueueGenerator):
    """Standard grid — same shape as R01/R03/R14-R17, M13 included via band_top_n_pairs().

    Legacy note: unlike every other ported strategy, R07 has NO dedicated
    backtest/generate_r7_queue.py in the legacy codebase (see
    docs/CODE_TRACEABILITY.md) — its queues were built ad-hoc
    (r7_crash_aware_weekly_7d.json, phaseB_r7_*.json, ...). This generator
    is the first single source of truth for R07's parameter grid.

    band_id=7 (M7) is EXCLUDED — common/benchmark.py has no resolved
    benchmark index for M7 (see that module), so crash detection would
    fail at runtime for band 7 today.
    """

    strategy_family = STRATEGY_CODE

    BANDS = [2, 4, 9, 10, 12, 13]  # 7 excluded — see class docstring
    LOOKBACK_MONTHS = [3, 6, 9, 12]
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]

    def __init__(self, start_date: str = "2009-01-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        jobs = self.simple_momentum_grid(
            strategy_code=STRATEGY_CODE,
            rank_method=RANK_METHOD,
            bands=self.BANDS,
            lookback_months=self.LOOKBACK_MONTHS,
            rebalance_cadences=self.REBALANCE_CADENCES,
            start_date=self.start_date,
            end_date=self.end_date,
            filter_presets=self.FILTER_PRESETS,
            crash_regime_enabled=True,
        )
        for job in jobs:
            job["crash_disable_buys"] = DEFAULT_CRASH_DISABLE_BUYS
            job["crash_reduce_sizing"] = None
            job["crash_drawdown_threshold"] = DEFAULT_DRAWDOWN_THRESHOLD
            job["crash_vol_percentile_threshold"] = DEFAULT_VOL_PERCENTILE_THRESHOLD
            job["crash_vol_lookback_days"] = DEFAULT_VOL_LOOKBACK_DAYS
        return jobs

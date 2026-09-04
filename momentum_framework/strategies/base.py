"""
StrategyBase - shared boilerplate for concrete strategy files.

Not a requirement (strategies only need to satisfy the StrategyAdapter
protocol), but every current strategy (R01, R03, R07-R17) benefits from the
same describe()/validate() plumbing, so it lives here once.
"""

from typing import Any, Dict, FrozenSet, List, Optional, Type

import pandas as pd

from momentum_framework.backtesting.adapter import Signal, StrategyAdapter
from momentum_framework.common.position_weighting import InverseVolatilityWeighting, WeightingScheme
from momentum_framework.common.signals import TrailingMomentumSignal


class StrategyBase(StrategyAdapter):
    """
    Convenience base for strategy files. Subclasses set strategy_code and
    rank_method as class attributes and implement rebalance().

    position_sizing (added 2026-09-04, explicit user instruction: every
    strategy needs an "equal weight" and a "weighted" variant, not just
    R14-R17): "equal" (default) leaves Signal.size_multiplier untouched
    (Portfolio splits capital evenly). "inverse_volatility" recomputes it
    via the SAME InverseVolatilityWeighting mechanism WeightedMomentum
    Strategy already uses for R14 — applied here generically, in
    BacktestOrchestrator.run_native() AFTER rebalance() returns, so it
    works for any strategy's own ranking signal (R10's industry_momentum,
    R11's pct_of_52wk_high, etc.) without that strategy file needing to
    know about weighting at all. Deliberately NOT wired into R14-R17
    themselves — those already have their own (different) weighting
    schemes baked in via WeightedMomentumStrategy; double-applying a
    second weighting pass on top of theirs would silently distort a
    result nobody asked for. See size_signals() below for the mechanism.
    """

    #: Research citation / paper this strategy implements, if any —
    #: required so a reviewer can verify the implementation against the
    #: published methodology (see feedback_strategy_audit_external_validation).
    citation: str = ""

    #: Set True by WeightedMomentumStrategy subclasses (R14-R17) — tells
    #: the orchestrator's size_signals() call to skip them, since they
    #: already size their own signals inline (see class docstring above).
    has_own_weighting: bool = False

    def __init__(self, band_id: int, top_n: int, lookback_months: int,
                 rebalance_cadence_days: int, position_sizing: str = "equal",
                 sizing_weight_lookback_days: int = 126, **kwargs: Any):
        super().__init__(band_id, top_n, lookback_months, rebalance_cadence_days,
                          position_sizing=position_sizing, **kwargs)
        if position_sizing not in ("equal", "inverse_volatility"):
            raise ValueError(f"unknown position_sizing {position_sizing!r} — must be 'equal' or 'inverse_volatility'")
        self.position_sizing = position_sizing
        self._sizing_scheme: Optional[WeightingScheme] = (
            InverseVolatilityWeighting(lookback_days=sizing_weight_lookback_days)
            if position_sizing == "inverse_volatility" else None
        )

    def size_signals(self, signals: List[Signal], as_of_date: str, conn: Any) -> List[Signal]:
        """Applied by the orchestrator right after rebalance() returns (see
        BacktestOrchestrator.run_native()). No-op for "equal" sizing, for
        R14-R17 (has_own_weighting=True), or when there's nothing to size.
        """
        if self.has_own_weighting or self._sizing_scheme is None or not signals:
            return signals
        buy_tickers = [s.ticker for s in signals if s.action == "buy"]
        if not buy_tickers:
            return signals
        weights = self._sizing_scheme.compute_weights(buy_tickers, as_of_date, conn)
        sized: List[Signal] = []
        for s in signals:
            if s.action != "buy":
                sized.append(s)
                continue
            w = weights.get(s.ticker)
            if w is None:
                # Scheme couldn't measure this ticker's volatility (insufficient
                # history) — dropped, not defaulted, matching WeightingScheme's
                # own documented convention (see WeightedMomentumStrategy.rebalance()).
                continue
            s.size_multiplier = w
            sized.append(s)
        return sized

    def validate_params(self) -> None:
        """Raise if this strategy's parameters are internally inconsistent.
        Subclasses should override to add strategy-specific checks (e.g.
        R11 requiring select_lowest=True) and call super() first."""
        if self.strategy_code == "UNSET":
            raise ValueError(f"{self.__class__.__name__} must set strategy_code")
        if self.rank_method == "unset":
            raise ValueError(f"{self.__class__.__name__} must set rank_method")

    def describe(self) -> Dict[str, Any]:
        self.validate_params()
        base = super().describe()
        if self.citation:
            base["citation"] = self.citation
        return base


class WeightedMomentumStrategy(StrategyBase):
    """
    Shared execution for R14/R15/R16/R17: rank the (band-scoped) universe
    by trailing return, select the top_n, then size each position via
    `weighting_scheme_class` — the ONLY thing that differs between the
    four. See common/position_weighting.py's module docstring for why
    this was split out of what used to be R0's `weight_method` parameter.

    A strategy file subclassing this only needs to set strategy_code,
    rank_method, and weighting_scheme_class as class attributes — see
    strategies/r14_inverse_volatility.py for the ~15-line result.
    """

    #: Set by subclasses — momentum_framework.common.position_weighting.*
    #: WeightingScheme itself is an abstract sentinel here (never
    #: instantiated — validate_params() below raises if a subclass
    #: doesn't override it), so mypy's abstract-class-assignment check is
    #: intentionally silenced, not a real typing gap.
    weighting_scheme_class: Type[WeightingScheme] = WeightingScheme
    has_own_weighting: bool = True

    def __init__(self, band_id: int, top_n: int, lookback_months: int,
                 rebalance_cadence_days: int, filter_preset: str = "all_risk",
                 weight_lookback_days: int = 126, **kwargs: Any):
        super().__init__(band_id, top_n, lookback_months, rebalance_cadence_days,
                          filter_preset=filter_preset,
                          weight_method=self.weighting_scheme_class.legacy_weight_method,
                          weight_lookback_days=weight_lookback_days, **kwargs)
        self.signal = TrailingMomentumSignal(lookback_months=lookback_months)
        self.weighting_scheme = self.weighting_scheme_class(lookback_days=weight_lookback_days)

    def validate_params(self) -> None:
        super().validate_params()
        if self.weighting_scheme_class is WeightingScheme:
            raise ValueError(f"{self.__class__.__name__} must set weighting_scheme_class")

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any,
                  held: FrozenSet[str], equity_curve: pd.Series) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        if winners.empty:
            return []

        weights = self.weighting_scheme.compute_weights(list(winners.index), as_of_date, conn)

        signals = []
        for rank, (ticker, score) in enumerate(winners.items()):
            ticker = str(ticker)
            weight = weights.get(ticker)
            if weight is None:
                # Weighting scheme couldn't measure this ticker's volatility
                # (insufficient history) — dropped, not defaulted, per
                # WeightingScheme.compute_weights()'s documented convention.
                continue
            signals.append(Signal(
                ticker=ticker, action="buy", conviction=score,
                rank=rank + 1, size_multiplier=weight,
            ))
        return signals

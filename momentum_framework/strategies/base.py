"""
StrategyBase - shared boilerplate for concrete strategy files.

Not a requirement (strategies only need to satisfy the StrategyAdapter
protocol), but every current strategy (R01, R03, R07-R17) benefits from the
same describe()/validate() plumbing, so it lives here once.
"""

from typing import Any, Dict, List, Type

from momentum_framework.backtesting.adapter import Signal, StrategyAdapter
from momentum_framework.common.position_weighting import WeightingScheme
from momentum_framework.common.signals import TrailingMomentumSignal


class StrategyBase(StrategyAdapter):
    """
    Convenience base for strategy files. Subclasses set strategy_code and
    rank_method as class attributes and implement rebalance().
    """

    #: Research citation / paper this strategy implements, if any —
    #: required so a reviewer can verify the implementation against the
    #: published methodology (see feedback_strategy_audit_external_validation).
    citation: str = ""

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
    weighting_scheme_class: Type[WeightingScheme] = WeightingScheme  # type: ignore[type-abstract]

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

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any) -> List[Signal]:
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

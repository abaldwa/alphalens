"""
R09: Moreira-Muir 4-Mode Volatility Scaling

Same shared ranking as R01/R03/R07/R08/R14-R17 (see common/signals.py::
TrailingMomentumSignal's module note) — like R08, the difference is a
PORTFOLIO-LEVEL exposure multiplier, but R09 offers 4 selectable modes
(inverse_volatility, inverse_variance, target_volatility,
downside_volatility — see common/portfolio_vol_scaling.py) instead of
R08's single fixed vol-target formula.

Ported from backtest/adapters/momentum_adapter.py's vol_scaling_mode
branch + features/volatility_scaling.py's portfolio-level dispatch
functions (NOT the "_per_ticker" variants — those are R14-R17's
mechanism, a different, already-ported code path).

NOTE (correction, 2026-09-04): R09 is NOT the default paper-trading
strategy — an earlier session turn assumed this from a since-corrected
CLAUDE.md description. Do not treat R09 as special-cased "the default"
without the user saying so directly.

regime_switching_enabled (B-027) IS NOW PORTED, 2026-09-04 —
common/regime_detection.py (majority-vote EMA/RSI/volatility ensemble)
+ common/benchmark.py (band-attached benchmark equity curve, per
explicit user instruction: "Index Equity Curve is to be attached to a
band and not to a strategy"). Regime detection runs on the STRATEGY'S
OWN BAND's benchmark index (e.g. band_id=2 -> Nifty 50), not a
per-strategy-supplied series — consistent with R07's crash detection
using the same band-benchmark mapping.

Mode selection when regime_switching_enabled=True (ported unchanged
from the legacy `_select_vol_scaling_mode()`):
    Bull   -> inverse_volatility (aggressive)
    Bear   -> downside_volatility (conservative)
    Choppy -> the configured vol_scaling_mode (fallback)

Discovered while porting: the legacy adapter's own import
(`backtest.core.regime_detection.EnsembleRegimeDetector`) points at a
module that DOES NOT EXIST — the real class lives in
`contracts/regime_detector.py`. That means regime_switching_enabled was
silently disabled (caught ImportError) in the legacy system too; this
port is the first time it actually runs. See common/regime_detection.py
for the full explanation.

ARCHITECTURE NOTE: the exposure multiplier (both the base vol_scaling_mode
math AND regime detection) needs update_portfolio_equity() fed by a
native orchestrator (backtesting/orchestrator.py) — ported 2026-09-04,
see that module.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.portfolio_vol_scaling import VOL_SCALING_DISPATCH
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R09"
RANK_METHOD = "trailing_return"

VALID_MODES = frozenset(VOL_SCALING_DISPATCH.keys())
DEFAULT_MODE = "inverse_volatility"
DEFAULT_LOOKBACK_DAYS = 126
DEFAULT_LEVERAGE_CAP = 1.0

# Regime -> vol_scaling_mode override, ported unchanged from legacy
# _select_vol_scaling_mode(). "Choppy" (and regime_switching_enabled=False)
# fall through to the configured self.vol_scaling_mode.
REGIME_MODE_OVERRIDE = {
    "Bull": "inverse_volatility",
    "Bear": "downside_volatility",
}


class R09MMVolScale(StrategyBase):
    """Trailing-return momentum with Moreira-Muir 4-mode portfolio vol-scaling."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "Moreira & Muir (2017), Journal of Finance"

    def __init__(
        self,
        band_id: int,
        top_n: int,
        lookback_months: int,
        rebalance_cadence_days: int,
        filter_preset: str = "all_risk",
        vol_scaling_mode: str = DEFAULT_MODE,
        vol_scaling_lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        vol_scaling_leverage_cap: Optional[float] = DEFAULT_LEVERAGE_CAP,
        vol_target_pct: float = 0.15,  # only consumed if the ACTIVE mode is target_volatility
        regime_switching_enabled: bool = False,
        **kwargs: Any,
    ):
        if vol_scaling_mode not in VALID_MODES:
            raise ValueError(f"vol_scaling_mode={vol_scaling_mode!r} not one of {sorted(VALID_MODES)}")
        super().__init__(
            band_id, top_n, lookback_months, rebalance_cadence_days,
            filter_preset=filter_preset, vol_scaling_mode=vol_scaling_mode,
            vol_scaling_lookback_days=vol_scaling_lookback_days,
            vol_scaling_leverage_cap=vol_scaling_leverage_cap,
            vol_target_pct=vol_target_pct,
            regime_switching_enabled=regime_switching_enabled,
            **kwargs,
        )
        self.signal = TrailingMomentumSignal(lookback_months=lookback_months)
        self.vol_scaling_mode = vol_scaling_mode
        self.vol_scaling_lookback_days = vol_scaling_lookback_days
        self.vol_scaling_leverage_cap = vol_scaling_leverage_cap
        self.vol_target_pct = vol_target_pct
        self.regime_switching_enabled = regime_switching_enabled
        self._equity_history: Optional[pd.Series] = None
        self._regime_series: Optional[pd.Series] = None  # lazily loaded, cached per instance

    def update_portfolio_equity(self, as_of_date: str, equity: float) -> None:
        # Built via concat, not `.loc[ts] = equity` in-place assignment —
        # pandas-stubs' loc-assignment overloads are ambiguous for growing
        # an empty float Series by a new Timestamp key (flags differently
        # across pandas-stubs versions); concat has one unambiguous signature.
        ts = pd.Timestamp(as_of_date)
        new_point = pd.Series([equity], index=[ts])
        if self._equity_history is None:
            self._equity_history = new_point
        else:
            self._equity_history = pd.concat([self._equity_history, new_point])

    def _detect_regime(self, as_of_date: str, conn: Any) -> str:
        """
        Band-attached regime (see module docstring) — loaded once per
        instance and cached, not re-queried every rebalance call.
        """
        if self._regime_series is None:
            from momentum_framework.common.benchmark import load_benchmark_equity_curve
            from momentum_framework.common.regime_detection import detect_ensemble_regime

            close = load_benchmark_equity_curve(self.band_id, conn)
            self._regime_series = detect_ensemble_regime(close) if not close.empty else pd.Series(dtype=object)

        ts = pd.Timestamp(as_of_date)
        if ts in self._regime_series.index:
            return str(self._regime_series.loc[ts])
        return "Choppy"

    def _active_mode(self, as_of_date: str, conn: Any) -> str:
        if not self.regime_switching_enabled:
            return self.vol_scaling_mode
        regime = self._detect_regime(as_of_date, conn)
        return REGIME_MODE_OVERRIDE.get(regime, self.vol_scaling_mode)

    def _exposure_multiplier(self, as_of_date: str, conn: Any) -> float:
        if self._equity_history is None or self._equity_history.empty:
            return 1.0
        active_mode = self._active_mode(as_of_date, conn)
        scaling_fn = VOL_SCALING_DISPATCH[active_mode]
        kwargs: Dict[str, Any] = {
            "lookback_days": self.vol_scaling_lookback_days,
            "leverage_cap": self.vol_scaling_leverage_cap,
        }
        if active_mode == "target_volatility":
            kwargs["target_vol"] = self.vol_target_pct
        try:
            mult_series = scaling_fn(self._equity_history, **kwargs)
        except (ValueError, KeyError):
            return 1.0
        ts = pd.Timestamp(as_of_date)
        if ts in mult_series.index:
            return float(mult_series.loc[ts])
        if len(mult_series) > 0:
            return float(mult_series.iloc[-1])
        return 1.0

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        if winners.empty:
            return []

        exposure = self._exposure_multiplier(as_of_date, conn)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=score,
                   rank=rank + 1, size_multiplier=exposure)
            for rank, (ticker, score) in enumerate(winners.items())
        ]


class R09QueueGenerator(QueueGenerator):
    """
    Standard grid — same shape as R01/R03/R07/R08/R14-R17, M13 included.
    Sweeps all 4 vol_scaling_mode values (legacy generate_r9_queue.py ran
    a single fixed mode per job set; this generator covers all 4 as a
    genuine parameter dimension, matching how R14-R17 sweep weight_method).

    band_id=7 (M7) is EXCLUDED here, not just from regime-switching —
    common/benchmark.py has no resolved benchmark for M7 at all (see that
    module), so a regime-switching sweep including band 7 would fail at
    runtime the moment regime detection is actually exercised.
    """

    strategy_family = STRATEGY_CODE

    BANDS = [2, 4, 9, 10, 12, 13]  # 7 excluded — see class docstring
    LOOKBACK_MONTHS = [3, 6, 9, 12]
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]
    VOL_SCALING_MODES = sorted(VALID_MODES)

    def __init__(self, start_date: str = "2009-01-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        jobs = []
        for mode in self.VOL_SCALING_MODES:
            extra = {
                "vol_scaling_lookback_days": DEFAULT_LOOKBACK_DAYS,
                "vol_scaling_leverage_cap": DEFAULT_LEVERAGE_CAP,
                "regime_switching_enabled": True,
            }
            if mode == "target_volatility":
                extra["vol_target_pct"] = 0.15
            jobs.extend(self.simple_momentum_grid(
                strategy_code=STRATEGY_CODE,
                rank_method=RANK_METHOD,
                bands=self.BANDS,
                lookback_months=self.LOOKBACK_MONTHS,
                rebalance_cadences=self.REBALANCE_CADENCES,
                start_date=self.start_date,
                end_date=self.end_date,
                filter_presets=self.FILTER_PRESETS,
                vol_scaling_mode=mode,
                extra_fields=extra,
            ))
        return jobs

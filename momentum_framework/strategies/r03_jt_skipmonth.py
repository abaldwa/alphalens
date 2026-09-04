"""
R03: Jegadeesh-Titman Momentum with 1-Month Skip

Citation: Jegadeesh, N. & Titman, S. (1993). "Returns to Buying Winners and
Selling Losers: Implications for Stock Market Efficiency." Journal of Finance.

Identical to R01 (r01_trailing_momentum.py) EXCEPT the trailing-return
ranking is computed skip_months before as_of_date, not at as_of_date
itself — the J&T 1993 "skip-month" rule, which avoids the short-term
reversal effect (microstructure noise / bid-ask bounce) contaminating the
momentum signal.

WHY THIS IS A SEPARATE FILE FROM R01, NOT A skip_months PARAMETER ON IT:
on 2026-09-04, backtest/generate_r1_full_queue.py added skip_months=1 to
what was still labeled "the R1 generator", and the legacy naming function
(strategies/momentum_identity.py::registry_name()) renames ANY momentum
job with skip_months>0 to "R3" regardless of what generated it. Because
one file was producing jobs under two different effective identities,
the R1 vs R3 distinction became invisible to whoever was reading the
generator's filename. Two files, one file per strategy_code, makes that
mistake structurally impossible: R01TrailingMomentum hardcodes
SKIP_MONTHS=0, this file hardcodes SKIP_MONTHS=1, and nothing here reads
the other's constant. (Strategy numbers zero-padded to R01/R03/... on
2026-09-04 — see project_r_number_zero_padding memory.)
"""

from typing import Any, Dict, List

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R03"
RANK_METHOD = "trailing_return"
SKIP_MONTHS = 1  # the defining difference from R01 — never set this to 0 here
TRADING_DAYS_PER_MONTH = 21


class R03JTSkipMonth(StrategyBase):
    """J&T momentum: trailing-return ranking, computed 1 month before as_of_date."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "Jegadeesh & Titman (1993), Journal of Finance"

    def __init__(self, band_id: int, top_n: int, lookback_months: int,
                 rebalance_cadence_days: int, filter_preset: str = "all_risk",
                 **kwargs: Any):
        super().__init__(band_id, top_n, lookback_months, rebalance_cadence_days,
                          filter_preset=filter_preset, skip_months=SKIP_MONTHS, **kwargs)
        self.signal = TrailingMomentumSignal(lookback_months=lookback_months)
        self.skip_days = SKIP_MONTHS * TRADING_DAYS_PER_MONTH

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any) -> List[Signal]:
        """
        Ranks `universe` by trailing return as of (as_of_date - skip_days),
        not as_of_date itself — see module docstring for why the skip
        exists. The caller is responsible for resolving as_of_date to the
        actual prior trading date (see backtest/adapters/momentum_adapter.py's
        cache-offset logic for the reference implementation).
        """
        offset_date = self._offset_trading_date(as_of_date, self.skip_days, conn)
        scores = self.signal.compute(conn, universe, offset_date, self.signal.lookback_days)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=score, rank=rank + 1)
            for rank, (ticker, score) in enumerate(winners.items())
        ]

    @staticmethod
    def _offset_trading_date(as_of_date: str, skip_days: int, conn: Any) -> str:
        """Resolve the trading date `skip_days` sessions before as_of_date."""
        row = conn.execute(
            """
            SELECT date FROM (
                SELECT DISTINCT date FROM ohlcv_adjusted
                WHERE date <= ?
                ORDER BY date DESC
                LIMIT 1 OFFSET ?
            )
            """,
            [as_of_date, skip_days],
        ).fetchone()
        if row is None:
            raise ValueError(f"Not enough trading history before {as_of_date} to skip {skip_days} days")
        return str(row[0])


class R03QueueGenerator(QueueGenerator):
    """
    Standard R03 grid — matches the Sept 4 2026 r1_full_campaign_216 queue's
    ACTUAL parameters (it set skip_months=1, so under the legacy naming
    scheme every one of its 216 jobs was already "R3", not "R1", no matter
    what the file was called).
    """

    strategy_family = STRATEGY_CODE

    # See R01QueueGenerator.BANDS comment — M13 (full 800-stock universe)
    # uses a wider top_n set than the partitioned bands, resolved via
    # band_top_n_pairs() from common/universe.py::TOP_N_BY_BAND.
    BANDS = [2, 4, 7, 9, 10, 12, 13]
    LOOKBACK_MONTHS = [3, 6, 9, 12]
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]

    def __init__(self, start_date: str = "2009-04-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        jobs = []
        for band_id, top_n in self.band_top_n_pairs(self.BANDS):
            for lookback in self.LOOKBACK_MONTHS:
                for cadence in self.REBALANCE_CADENCES:
                    for filter_preset in self.FILTER_PRESETS:
                        jobs.append({
                            "kind": "orchestrator",
                            "channel": "momentum",
                            "start_date": self.start_date,
                            "end_date": self.end_date,
                            "rank_band_id": band_id,
                            "top_n": top_n,
                            "lookback_months": lookback,
                            "rebalance_cadence_days": cadence,
                            "rank_method": RANK_METHOD,          # explicit, always
                            "crash_regime_enabled": False,       # explicit, always
                            "skip_months": SKIP_MONTHS,          # explicit, always 1
                            "filter_preset": filter_preset,
                            "strategy_family": STRATEGY_CODE,
                            "capital_mode": "lump",
                            "initial_capital": 1_000_000,
                            "max_tickers": 800,
                            "min_history_days": 60,
                            "exit_variant": "unconstrained",
                        })
        return jobs

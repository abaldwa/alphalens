"""
Crash Regime Detection — Daniel-Moskowitz-style overlay used by R07.

Direct port of features/momentum_signal.py::crash_regime_detector() —
same formula, unchanged, so R07's crash dates match the legacy engine's
exactly. This is the ONE piece of R07 kept as a faithful line-for-line
translation rather than a framework-native rebuild, because the formula
itself (drawdown + elevated rolling vol, both against a benchmark) has no
"shared ranking" analog to unify with — it's a standalone regime signal,
not a stock-ranking signal.
"""

from typing import Any, Optional
import logging

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_REGIME_LOOKBACK_DAYS = 252

#: Single shared "is the market in a crash" benchmark, used identically by
#: EVERY band and EVERY crash-aware strategy (R07, R11/R12/R13's guard) —
#: explicit user instruction 2026-09-06 ("we will use 1 common Crash
#: detector across Bands"), replacing the earlier per-band-benchmark
#: design (common/benchmark.py's BAND_BENCHMARK_INDEX), which let the same
#: real-world event register as "crash" for one band and not another.
#:
#: Nifty 500 chosen over Nifty 50 after checking both against 8 known
#: Indian equity crises (2008 GFC through 2022) at real historical
#: drawdowns: Nifty 50 (large-cap only) understates stress relevant to
#: this framework's actual bands, which are overwhelmingly mid/small/
#: micro-cap (M4/M9/M10/M12) — e.g. the 2013 rupee crisis was -26.4% on
#: Nifty 500 vs only -16.3% on Nifty 50, and 2022 was -18.5% vs -17.2%.
#: Verified 2026-09-06 (docs/MASTER_ISSUES_CATALOG_2026_09_06.md).
MARKET_WIDE_BENCHMARK_INDEX = "Nifty 500"

#: Validated 2026-09-06 against real Nifty 500 drawdowns for all 8 known
#: Indian equity crises (2008 GFC, 2011, 2013, Aug-2015, 2016, 2018, 2020,
#: 2022) — -12.5% is the tightest threshold that still flags every one of
#: them (Aug 2015's actual max drawdown on Nifty 500 was -13.9%; anything
#: stricter than -13.9% starts missing it). Explicit user decision
#: 2026-09-06: recall (catching every real crash) weighted over precision
#: (fewer flagged days) — a missed crash lets a mean-reversion strategy
#: buy straight into a collapse (the exact B6 failure mode), while a false
#: positive only costs one skipped rebalance of new buys.
DEFAULT_CRASH_DRAWDOWN_THRESHOLD = -0.125
DEFAULT_CRASH_VOL_PERCENTILE_THRESHOLD = 0.75
DEFAULT_CRASH_VOL_LOOKBACK_DAYS = 20

_market_wide_equity_cache: Optional[pd.Series] = None


def load_market_wide_equity_curve(conn: Any) -> pd.Series:
    """
    The MARKET_WIDE_BENCHMARK_INDEX close-price series (pd.Series indexed
    by date), band-independent — every crash-aware strategy instance,
    regardless of which band it trades, consults this SAME series. Cached
    at module level (read-only reference data, identical for the life of
    a process) to avoid re-querying index_ohlcv once per strategy
    instance across a campaign.
    """
    global _market_wide_equity_cache
    if _market_wide_equity_cache is None:
        df = conn.execute(
            "SELECT date, close FROM index_ohlcv WHERE index_name = ? ORDER BY date",
            [MARKET_WIDE_BENCHMARK_INDEX],
        ).fetch_df()
        if df.empty:
            _market_wide_equity_cache = pd.Series(dtype=float)
        else:
            df["date"] = pd.to_datetime(df["date"])
            _market_wide_equity_cache = df.set_index("date")["close"]
    return _market_wide_equity_cache


def crash_regime_detector(
    equity_curve: pd.Series,
    drawdown_threshold: float = -0.15,
    vol_percentile_threshold: float = 0.75,
    lookback_days: int = 252,
    vol_lookback_days: int = 20,
) -> pd.Series:
    """
    Detects "crash regime" dates when `equity_curve` is in drawdown AND
    its volatility is elevated relative to its own trailing baseline.

    A date enters crash regime when BOTH:
    1. `equity_curve` is within drawdown_threshold from its running peak
       (e.g., -0.15 = down 15% from the highest value seen so far)
    2. Rolling volatility over vol_lookback_days exceeds the
       vol_percentile_threshold percentile of the trailing lookback_days
       volatility distribution

    [Phase 7 fix, 2026-09-02 — carried over from the legacy port] Pass a
    real market-index equity curve (e.g. Nifty 500 level series), not the
    strategy's own P&L — crash regime is a market-wide signal. R07's
    strategy file below only supports this benchmark-driven mode; the
    legacy adapter's self-referential fallback (using the strategy's own
    equity curve when no benchmark was supplied) is NOT ported — the
    legacy code itself documents benchmark_equity as the preferred mode,
    and self-referential mode requires portfolio-value plumbing the
    framework's StrategyAdapter doesn't have yet (see
    docs/CODE_TRACEABILITY.md's R07 row).

    Returns
    -------
    pd.Series (index=date, dtype=bool). Missing/insufficient data -> False
    (never exclude on unknown regime).
    """
    if equity_curve.empty or len(equity_curve) < lookback_days:
        return pd.Series(dtype=bool)

    running_peak = equity_curve.expanding().max()
    drawdown = (equity_curve - running_peak) / running_peak
    in_drawdown = drawdown <= drawdown_threshold

    daily_returns = equity_curve.pct_change()
    rolling_vol = daily_returns.rolling(window=vol_lookback_days, min_periods=vol_lookback_days).std()
    vol_percentile = rolling_vol.rolling(window=lookback_days, min_periods=1).quantile(vol_percentile_threshold)
    elevated_vol = rolling_vol > vol_percentile

    crash_regime = (in_drawdown & elevated_vol).fillna(False)
    return crash_regime.astype(bool)


class CrashRegimeGuardMixin:
    """
    Shared "disable new buys during a crash regime" guard — used by BOTH
    R07 (buy-disable + trim, Category B3) and the mean-reversion
    strategies R11/R12/R13 (buy-disable only, Category B6) that had NO
    regime defense at all before 2026-09-06 — buying "oversold" names
    during an actual market-wide crash/structural collapse is exactly the
    failure mode behind their worst tradebook losses (ADANIENT -82.77%
    2015-06-03, YESBANK/PNB 2018 banking crisis, INDUSINDBK 2020 pandemic
    — see docs/TRADEBOOK_ROOT_CAUSE_ANALYSIS_2026_09_06.md).

    Uses ONE shared market-wide benchmark (MARKET_WIDE_BENCHMARK_INDEX,
    Nifty 500) for every band, NOT common/benchmark.py's per-band
    resolution — explicit user instruction 2026-09-06: "we will use 1
    common Crash detector across Bands" (see MARKET_WIDE_BENCHMARK_INDEX's
    docstring above for why Nifty 500 over Nifty 50, and
    DEFAULT_CRASH_DRAWDOWN_THRESHOLD's for the -12.5% threshold). Every
    band-scoped strategy instance therefore agrees on which dates are "in
    a crash," regardless of which band it trades — band-scoped RANKING is
    untouched, this only concerns the crash gate.

    Host class's __init__ must set: self.crash_regime_enabled (bool),
    self.crash_drawdown_threshold, self.crash_vol_percentile_threshold,
    self.crash_vol_lookback_days. self._benchmark_equity starts unset here
    and is lazily loaded + cached per instance on first call (on top of
    load_market_wide_equity_curve()'s own module-level cache, so this is
    just avoiding a repeated dict/Series lookup, not a repeated DB query).
    """

    crash_regime_enabled: bool
    crash_drawdown_threshold: float
    crash_vol_percentile_threshold: float
    crash_vol_lookback_days: int
    _benchmark_equity: Optional[pd.Series] = None

    def _in_crash_regime(self, as_of_date: str, conn: Any) -> bool:
        if not self.crash_regime_enabled:
            return False
        if self._benchmark_equity is None:
            self._benchmark_equity = load_market_wide_equity_curve(conn)
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
        except (ValueError, KeyError) as e:
            logger.warning(
                "crash_regime_detector failed for %s (%s: %s) — degrading to 'no crash regime' for this call",
                self.__class__.__name__, type(e).__name__, e,
            )
            return False
        ts = pd.Timestamp(as_of_date)
        return bool(crash_series.get(ts, False))

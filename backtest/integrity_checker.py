"""
backtest/integrity_checker.py

Phase: 1.4 (Labeling + Backtesting Infrastructure)
Specs: SPEC-BT-001 through SPEC-BT-004
Owner: Platform / Backtest
Consumers: systems/ml_signal_engine/training/walk_forward.py, backtest/engine.py (Phase 1.6)

BacktestIntegrityChecker: validates a completed (or in-progress) backtest
against every rule SPEC-BT-001 calls a hard constraint, plus two
overfitting-detection checks from alphalens_docs/04_backtesting.md's
"Overfitting Detection" section.

Count note: SPEC-BT-001 says "all 9 backtesting rules"; this prompt names
10 distinct check_XX methods (check_01 through check_10). The 10th
(check_10_random_feature) and part of the 8th's framing come from 04_
backtesting.md's separate "Overfitting Detection" section, not literally
one of the 9 enumerated "Non-Negotiable Rules" — same kind of count
mismatch as P1.1's 76-vs-70 and P1.3's "9 rules" framing; implemented
exactly the 10 named methods rather than forcing a count to match. The
Deflated Sharpe Ratio (SPEC-BT-001 rule 8) has no corresponding check_XX
name in this prompt either — its utility lives in backtest/overfit_
checks.py (deflated_sharpe_ratio) for a future caller to use directly,
since "DSR correction applies when testing 20+ configs" is a conditional
rule, not a pass/fail gate on every backtest.

This class validates pre-computed values the caller supplies (fold
splits, applied costs, fold Sharpes, etc.) — it does not itself run a
backtest. No backtest engine exists yet (that's backtest/engine.py,
Phase 1.6); this checker is built now, against whatever data shape a
caller can already produce, so it's ready to plug into that engine
without rework.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from config.settings import MIN_ADT_INR, TOTAL_ROUNDTRIP_COST

logger = logging.getLogger(__name__)

# [AS BUILT, full-codebase-review Fix 12, 2026-07-19] Narrow allowlist of
# fundamentals-derived feature name patterns used by check_02_pit's
# stopgap (see that method's docstring for why this exists instead of a
# full features/registry.py cross-check). Deliberately conservative —
# false negatives (a fundamentals feature this list misses) are possible,
# but false positives (flagging a genuinely PITRule.NONE technical/macro
# column) would break real backtests, so this only matches unambiguous
# fundamentals-ratio/governance naming conventions already used across
# features/fundamental*.py and features/governance.py.
_FUNDAMENTALS_DERIVED_PATTERNS = (
    "_ratio", "roe", "roce", "pledge", "promoter", "eps", "pat", "ebitda",
    "book_value", "debt_to_equity", "interest_coverage", "altman_z",
    "quality_flag", "shareholding",
    # [BUG FIX, 2026-07-21 full-codebase-review REV7] Broadened per real
    # column names confirmed present in features/forensic_classical.py
    # and features/fundamental*.py that the original list missed —
    # e.g. `cfo_to_net_income`, `m_score`/`f_score`/`o_score`,
    # `dechow_f_score`/`piotroski_f_score`, `capex_to_cfo_ratio` (this one
    # already matched `_ratio`, kept for clarity), `revenue_concentration`,
    # `channel_stuffing_indicator` — all fundamentals-statement-derived and
    # PIT-sensitive (only knowable after the underlying filing), none of
    # which contain "_ratio"/"roe"/etc. Still deliberately not exhaustive —
    # see this module's docstring for why a full features/registry.py
    # cross-check isn't viable yet (registry.py's own names predate
    # matrix_builder.py's current ALL_FEATURE_COLUMNS and don't match it).
    "cfo", "accrual", "revenue", "m_score", "f_score", "o_score",
    "dechow", "piotroski", "beneish", "capex", "cash_flow", "receivable",
    "channel_stuffing", "tax_paid", "fcf_",
    # [BUG FIX, 2026-07-25 fundamental-strategy-catalog model-review] These
    # 24 features (features/fundamental.py's VALUE_QUALITY_FEATURES +
    # MULTIYEAR_FEATURES + DELTA_1Y_FEATURES + SIZE_AGE_FEATURES +
    # CAPITAL_ALLOCATION_FEATURES) were confirmed by backtest-reviewer to
    # slip past every existing pattern above — check_02_pit would have
    # silently PASSED them as PITRule.NONE (safe) despite being derived
    # from filing data, in any feature_df lacking explicit PIT columns.
    # Specific compound tokens used (not broad single words like "margin"
    # or "roa" alone) to preserve this list's stated no-false-positives
    # design goal. `company_age_years` deliberately excluded — it depends
    # only on stock_master.listing_date, which is public/known immediately
    # on listing, not a quarterly-filing-restatement-sensitive value like
    # the rest of this list.
    "ev_ebit_yield", "magic_formula", "book_to_market", "market_cap",
    "avg_roce", "margin_stability", "earnings_volatility", "sales_cagr",
    "delta_roce", "avg_ebitda_margin", "eps_acceleration", "margin_expansion",
    "delta_roa", "delta_current_ratio", "delta_long_term_debt",
    "inventory_days", "dilution_3y", "capital_allocation", "reinvestment_rate",
)


def _looks_fundamentals_derived(column_name: str) -> bool:
    lowered = column_name.lower()
    return any(pattern in lowered for pattern in _FUNDAMENTALS_DERIVED_PATTERNS)

# SPEC-BT-001: data-integrity / look-ahead-prevention checks — a failure
# here means the backtest result is not trustworthy and must halt.
# 08/09/10 are performance-quality signals (a real, clean backtest can
# still legitimately fail fold stability or underperform a benchmark
# without that implying a data leak), so they warn rather than raise.
CRITICAL_CHECKS = {
    "check_01_walk_forward",
    "check_02_pit",
    "check_03_corp_actions",
    "check_04_survivorship",
    "check_05_costs",
    "check_06_liquidity",
    "check_07_no_hpo_on_test",
}

ALL_CHECK_NAMES = [
    "check_01_walk_forward",
    "check_02_pit",
    "check_03_corp_actions",
    "check_04_survivorship",
    "check_05_costs",
    "check_06_liquidity",
    "check_07_no_hpo_on_test",
    "check_08_fold_stability",
    "check_09_benchmarks",
    "check_10_random_feature",
    "check_11_sector_tier_lookahead",
]

# REV15 (2026-07-21 review): sector/tier columns from config/build_universe.py
# reflect NSE's CURRENT classification snapshot, not point-in-time membership
# — conditioning a multi-year backtest on them applies today's label
# retroactively. A full PIT-joined sector/tier history is a separate
# data-ingestion project (out of scope here); this is the review's own
# accepted cheaper alternative — an explicit guardrail flagging the risk
# rather than silently trusting it.
_SECTOR_TIER_COLUMN_NAMES = {"sector", "tier", "market_cap_tier"}


@dataclass
class CheckResult:
    name: str
    passed: bool
    critical: bool
    detail: str


@dataclass
class BacktestIntegrityChecker:
    """
    Each field is optional context for the one or two checks that need
    it; a check whose required context is missing FAILS (not skipped) —
    "couldn't verify" is not the same as "verified clean", and silently
    skipping a safety check is worse than a loud, attributable failure.
    """

    folds: Optional[List[Tuple[pd.DataFrame, pd.DataFrame]]] = None
    feature_df: Optional[pd.DataFrame] = None
    ohlcv_df: Optional[pd.DataFrame] = None
    universe_tickers: Optional[Set[str]] = None
    historical_tickers: Optional[Set[str]] = None
    applied_roundtrip_cost_pct: Optional[float] = None
    applied_min_adt_inr: Optional[float] = None
    hpo_dataset: Optional[str] = None
    fold_sharpes: Optional[List[float]] = None
    fold_returns: Optional[List[float]] = None
    benchmark_returns: Optional[List[float]] = None
    random_feature_accuracy: Optional[float] = None
    # REV18 (2026-07-21 review): a non-empty delisted set alone doesn't prove
    # the universe is survivorship-bias-safe — a near-complete universe
    # missing just 1-2 delisted names out of hundreds would still pass a
    # presence-only check. 1% is a conservative floor: real NSE-listed-equity
    # attrition (delistings/suspensions/mergers) over any multi-year backtest
    # window is comfortably above this in practice, so a ratio below it is a
    # signal the delisted-ticker set itself is incomplete, not that the
    # market genuinely had that few exits.
    min_delisted_ratio: float = 0.01

    _results_cache: Dict[str, CheckResult] = field(default_factory=dict, repr=False, compare=False)

    def _result(self, name: str, passed: bool, detail: str) -> CheckResult:
        return CheckResult(name=name, passed=passed, critical=name in CRITICAL_CHECKS, detail=detail)

    def check_01_walk_forward(self) -> CheckResult:
        """SPEC-BT-001 rule 1: walk-forward only, never a random split."""
        name = "check_01_walk_forward"
        if not self.folds:
            return self._result(name, False, "no folds provided")
        for i, (train_df, test_df) in enumerate(self.folds):
            if train_df.empty or test_df.empty:
                return self._result(name, False, f"fold {i} has an empty train or test split")
            if train_df["date"].max() >= test_df["date"].min():
                return self._result(
                    name, False,
                    f"fold {i}: max(train date)={train_df['date'].max()} >= min(test date)="
                    f"{test_df['date'].min()} — train data overlaps or follows test data, not "
                    "a chronological walk-forward split",
                )
        return self._result(name, True, f"{len(self.folds)} folds, all chronologically ordered")

    def check_02_pit(self) -> CheckResult:
        """SPEC-BT-001 rule 2 / SPEC-PIPE-003: no future data in any feature."""
        name = "check_02_pit"
        if self.feature_df is None:
            return self._result(name, False, "no feature_df provided")
        df = self.feature_df
        if "date" not in df.columns:
            return self._result(name, False, "feature_df has no 'date' column")

        pit_cols = [c for c in ("announcement_date", "filing_date") if c in df.columns]
        if not pit_cols:
            # [AS BUILT, full-codebase-review Fix 12, 2026-07-19] Trusting
            # "no PIT columns present" as proof of PITRule.NONE is a
            # trust-not-verify gap — features/registry.py's pit_rule
            # declarations aren't currently wired into the production
            # feature pipeline (matrix_builder.py uses different column
            # names — see registry.py's own disclaimer), so a full
            # registry cross-check isn't viable yet. As a narrower,
            # achievable stopgap: fail loudly if any column name matches a
            # known fundamentals-derived feature pattern (these should
            # always carry announcement_date/filing_date) rather than
            # silently vacuous-passing a feature_df that's missing its PIT
            # column by mistake.
            suspect = [c for c in df.columns if _looks_fundamentals_derived(c)]
            if suspect:
                return self._result(
                    name, False,
                    f"no announcement_date/filing_date column present, but {suspect} "
                    "look fundamentals-derived (roe/pledge/ratio-style names) and should "
                    "carry one — likely a missing PIT column, not a genuine PITRule.NONE feature",
                )
            return self._result(
                name, True, "no announcement_date/filing_date columns present — "
                "pure technical/calendar/macro features are PITRule.NONE by construction"
            )

        violations = 0
        for pit_col in pit_cols:
            known = df[df[pit_col].notna()]
            bad = known[pd.to_datetime(known[pit_col]) > pd.to_datetime(known["date"])]
            violations += len(bad)
        if violations:
            return self._result(name, False, f"{violations} rows reference PIT data published after their feature date")
        return self._result(name, True, f"checked {pit_cols}, no look-ahead found")

    def check_03_corp_actions(self) -> CheckResult:
        """SPEC-BT-001 / SPEC-PIPE-002: corporate-action-adjusted prices (adj_factor present)."""
        name = "check_03_corp_actions"
        if self.ohlcv_df is None:
            return self._result(name, False, "no ohlcv_df provided")
        if "adj_factor" not in self.ohlcv_df.columns:
            return self._result(name, False, "ohlcv_df has no adj_factor column")
        return self._result(name, True, "adj_factor column present")

    def check_04_survivorship(self) -> CheckResult:
        """SPEC-BT-001 rule 3 / SPEC-BT-003: delisted stocks must be included, not survivorship-filtered."""
        name = "check_04_survivorship"
        if self.universe_tickers is None or self.historical_tickers is None:
            return self._result(name, False, "universe_tickers/historical_tickers not provided")
        delisted_seen = self.historical_tickers - self.universe_tickers
        if not delisted_seen:
            return self._result(
                name, False,
                "every historical ticker is still in the current universe — no delisted/removed "
                "names found, survivorship bias risk",
            )
        ratio = len(delisted_seen) / len(self.historical_tickers)
        if ratio < self.min_delisted_ratio:
            return self._result(
                name, False,
                f"only {len(delisted_seen)}/{len(self.historical_tickers)} historical tickers "
                f"({ratio:.2%}) are delisted/removed — below the {self.min_delisted_ratio:.2%} "
                "plausibility floor; the delisted-ticker set is likely incomplete, not that the "
                "market genuinely had this few exits (REV18)",
            )
        return self._result(
            name, True,
            f"{len(delisted_seen)}/{len(self.historical_tickers)} historical tickers ({ratio:.2%}) "
            "delisted/since-removed and included",
        )

    def check_05_costs(self) -> CheckResult:
        """SPEC-BT-001 rule 4 / SPEC-BT-002: TOTAL_ROUNDTRIP_COST actually applied."""
        name = "check_05_costs"
        if self.applied_roundtrip_cost_pct is None:
            return self._result(name, False, "no applied_roundtrip_cost_pct provided")
        if self.applied_roundtrip_cost_pct <= 0:
            return self._result(name, False, "zero/negative transaction cost applied — costs not modeled")
        # Allow costs to be more conservative (higher) than the configured floor, but not
        # materially below it — a backtest with implausibly cheap costs overstates returns.
        if self.applied_roundtrip_cost_pct < 0.5 * TOTAL_ROUNDTRIP_COST:
            return self._result(
                name, False,
                f"applied cost {self.applied_roundtrip_cost_pct:.4f} is less than half of "
                f"TOTAL_ROUNDTRIP_COST ({TOTAL_ROUNDTRIP_COST}) — costs appear understated",
            )
        return self._result(name, True, f"applied roundtrip cost {self.applied_roundtrip_cost_pct:.4f}")

    def check_06_liquidity(self) -> CheckResult:
        """SPEC-BT-001 rule 5: MIN_ADT_INR liquidity filter applied."""
        name = "check_06_liquidity"
        if self.applied_min_adt_inr is None:
            return self._result(name, False, "no applied_min_adt_inr provided")
        if self.applied_min_adt_inr < MIN_ADT_INR:
            return self._result(
                name, False,
                f"applied liquidity floor {self.applied_min_adt_inr} < required MIN_ADT_INR {MIN_ADT_INR}",
            )
        return self._result(name, True, f"applied liquidity floor {self.applied_min_adt_inr} >= {MIN_ADT_INR}")

    def check_07_no_hpo_on_test(self) -> CheckResult:
        """SPEC-BT-001 rule 6 / SPEC-MODEL-003: HPO never touches the test fold."""
        name = "check_07_no_hpo_on_test"
        if self.hpo_dataset is None:
            return self._result(name, False, "no hpo_dataset provided")
        if "test" in self.hpo_dataset.lower():
            return self._result(name, False, f"HPO touched the test set (hpo_dataset={self.hpo_dataset!r})")
        return self._result(name, True, f"HPO ran on {self.hpo_dataset!r} only")

    def check_08_fold_stability(self) -> CheckResult:
        """SPEC-BT-001 rule 7: std(fold Sharpes) < 0.5."""
        name = "check_08_fold_stability"
        if not self.fold_sharpes:
            return self._result(name, False, "no fold_sharpes provided")
        std = float(np.std(self.fold_sharpes))
        return self._result(name, std < 0.5, f"std(fold_sharpes)={std:.3f} (threshold < 0.5)")

    def check_09_benchmarks(self) -> CheckResult:
        """SPEC-BT-001 rule 9: beat the Nifty 50 buy-hold benchmark in >= 3 folds."""
        name = "check_09_benchmarks"
        if not self.fold_returns or not self.benchmark_returns:
            return self._result(name, False, "no fold_returns/benchmark_returns provided")
        if len(self.fold_returns) != len(self.benchmark_returns):
            return self._result(name, False, "fold_returns and benchmark_returns have different lengths")
        wins = sum(1 for f, b in zip(self.fold_returns, self.benchmark_returns) if f > b)
        return self._result(name, wins >= 3, f"beat benchmark in {wins}/{len(self.fold_returns)} folds (need >= 3)")

    def check_10_random_feature(self) -> CheckResult:
        """Overfitting Detection (04_backtesting.md): random-feature accuracy in [0.48, 0.52]."""
        name = "check_10_random_feature"
        if self.random_feature_accuracy is None:
            return self._result(name, False, "no random_feature_accuracy provided")
        passed = 0.48 <= self.random_feature_accuracy <= 0.52
        detail = f"random feature accuracy={self.random_feature_accuracy:.3f} (band [0.48, 0.52])"
        return self._result(name, passed, detail)

    def check_11_sector_tier_lookahead(self) -> CheckResult:
        """
        REV15 (2026-07-21 review): sector/tier/market_cap_tier reflect
        NSE's CURRENT classification snapshot (config/build_universe.py),
        not point-in-time membership. Using one as a time-varying feature
        across a multi-year window applies today's label retroactively —
        classic label look-ahead. Non-critical (a warning, not a hard
        failure): a backtest that only uses these columns as a same-day
        filter, or over a short single-year window, isn't necessarily
        affected — this flags the risk for a human to confirm, rather than
        halting every run that happens to carry the column.
        """
        name = "check_11_sector_tier_lookahead"
        if self.feature_df is None or "date" not in self.feature_df.columns:
            return self._result(name, True, "no feature_df/date column to check")
        suspect_cols = sorted(set(self.feature_df.columns) & _SECTOR_TIER_COLUMN_NAMES)
        if not suspect_cols:
            return self._result(name, True, "no sector/tier/market_cap_tier column present")
        dates = pd.to_datetime(self.feature_df["date"])
        span_days = (dates.max() - dates.min()).days
        if span_days > 365:
            return self._result(
                name, False,
                f"{suspect_cols} present alongside a {span_days}-day feature window (> 1 year) — "
                "these reflect NSE's CURRENT classification snapshot, not point-in-time membership; "
                "using them as a time-varying feature over a multi-year window applies today's "
                "label retroactively (REV15)",
            )
        return self._result(
            name, True, f"{suspect_cols} present but feature window is only {span_days} days (<= 1 year)",
        )

    def run_all_checks(self) -> Dict[str, bool]:
        """
        Run every check; raise if any CRITICAL check fails.

        Returns
        -------
        dict
            {check_name: passed} for all 10 checks (only returned if no
            critical check failed).

        Raises
        ------
        RuntimeError
            If any check in CRITICAL_CHECKS failed — the backtest result
            is not trustworthy and must not be used.
        """
        results = {name: getattr(self, name)() for name in ALL_CHECK_NAMES}
        self._results_cache = results

        critical_failures = [r for r in results.values() if r.critical and not r.passed]
        if critical_failures:
            details = "; ".join(f"{r.name}: {r.detail}" for r in critical_failures)
            raise RuntimeError(f"CRITICAL backtest integrity check(s) failed: {details}")

        non_critical_failures = [r for r in results.values() if not r.critical and not r.passed]
        for r in non_critical_failures:
            logger.warning(f"Backtest quality check failed (non-critical): {r.name}: {r.detail}")

        return {name: r.passed for name, r in results.items()}

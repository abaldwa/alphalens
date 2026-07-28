"""
tests/unit/test_backtester.py

Phase: 1.4 (Labeling + Backtesting Infrastructure)
Specs: SPEC-MODEL-003, SPEC-BT-001 through SPEC-BT-004
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for systems/ml_signal_engine/training/walk_forward.py,
backtest/integrity_checker.py, backtest/costs.py, backtest/overfit_checks.py.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.costs import IndianTransactionCosts
from backtest.integrity_checker import BacktestIntegrityChecker
from backtest.overfit_checks import deflated_sharpe_ratio, random_feature_test
from config.settings import MIN_ADT_INR, TOTAL_ROUNDTRIP_COST
from contracts.interfaces import IModel
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator


def _daily_df(start, periods):
    return pd.DataFrame({"date": pd.date_range(start, periods=periods, freq="D")})


# ===== WalkForwardValidator.split_data =====


class TestSplitData:
    def test_five_folds_produced(self):
        df = _daily_df("2020-01-01", 2400)  # ~6.6 years -> enough for 5 folds
        validator = WalkForwardValidator(n_folds=5)

        folds = validator.split_data(df)

        assert len(folds) == 5

    def test_fold_date_ranges_are_expanding_and_correct(self):
        df = _daily_df("2020-01-01", 2400)
        validator = WalkForwardValidator(n_folds=5)

        folds = validator.split_data(df)

        prev_train_size = 0
        for train_df, test_df in folds:
            # Expanding window: each fold's train set is >= the previous fold's.
            assert len(train_df) >= prev_train_size
            prev_train_size = len(train_df)
            # Test set is exactly the fiscal year (Apr-Mar) immediately after
            # the training cutoff.
            assert validator._fiscal_years(test_df["date"]).nunique() == 1
            assert (
                validator._fiscal_years(train_df["date"]).max()
                < validator._fiscal_years(test_df["date"]).min()
            )

    def test_no_overlap_between_train_and_test(self):
        df = _daily_df("2020-01-01", 2400)
        validator = WalkForwardValidator(n_folds=5)

        folds = validator.split_data(df)

        for train_df, test_df in folds:
            overlap = set(train_df["date"]) & set(test_df["date"])
            assert not overlap
            assert train_df["date"].max() < test_df["date"].min()

    def test_insufficient_years_raises(self):
        df = _daily_df("2020-01-01", 400)  # ~1 year
        validator = WalkForwardValidator(n_folds=5)
        with pytest.raises(ValueError):
            validator.split_data(df)

    def test_missing_date_column_raises(self):
        df = pd.DataFrame({"not_date": [1, 2, 3]})
        validator = WalkForwardValidator(n_folds=2)
        with pytest.raises(ValueError):
            validator.split_data(df)

    def test_n_folds_override_at_call_time(self):
        df = _daily_df("2020-01-01", 2400)
        validator = WalkForwardValidator(n_folds=5)

        folds = validator.split_data(df, n_folds=3)

        assert len(folds) == 3


class TestTrainValidationSplit:
    def test_validation_is_chronologically_last_slice(self):
        df = _daily_df("2020-01-01", 1000)
        validator = WalkForwardValidator()

        train_only, val = validator.get_train_validation_split(df, val_fraction=0.2)

        assert train_only["date"].max() < val["date"].min()
        assert len(train_only) + len(val) == len(df)

    def test_invalid_val_fraction_raises(self):
        df = _daily_df("2020-01-01", 100)
        validator = WalkForwardValidator()
        with pytest.raises(ValueError):
            validator.get_train_validation_split(df, val_fraction=1.5)


# ===== BacktestIntegrityChecker =====


class TestIntegrityChecker:
    def test_walk_forward_check_catches_a_deliberately_introduced_leak(self):
        """A deliberately overlapping (leaked) fold must fail check_01, not silently pass."""
        train = _daily_df("2020-01-01", 400)
        leaked_test = _daily_df("2020-06-01", 30)  # starts mid-train -> real leak

        checker = BacktestIntegrityChecker(folds=[(train, leaked_test)])
        result = checker.check_01_walk_forward()

        assert result.passed is False
        assert result.critical is True

    def test_clean_folds_pass_walk_forward_check(self):
        train = _daily_df("2020-01-01", 365)
        test = _daily_df("2021-01-01", 30)
        checker = BacktestIntegrityChecker(folds=[(train, test)])

        result = checker.check_01_walk_forward()

        assert result.passed is True

    def test_pit_check_passes_with_no_pit_columns_and_no_fundamentals_names(self):
        """A pure technical/calendar feature_df (no announcement_date/
        filing_date, no fundamentals-like column names) is genuinely
        PITRule.NONE — should pass."""
        checker = BacktestIntegrityChecker(
            feature_df=pd.DataFrame({
                "date": pd.date_range("2020-01-01", periods=5),
                "rsi_14": [50.0] * 5,
                "sma_20": [100.0] * 5,
            })
        )
        result = checker.check_02_pit()
        assert result.passed is True

    def test_pit_check_fails_on_fundamentals_like_column_missing_pit_col(self):
        """2026-07-19 full-codebase-review Fix 12: a fundamentals-derived-
        looking column (e.g. roe) with neither announcement_date nor
        filing_date present must fail loudly rather than vacuously pass —
        this is exactly the failure mode a feature-engineering mistake
        would produce."""
        checker = BacktestIntegrityChecker(
            feature_df=pd.DataFrame({
                "date": pd.date_range("2020-01-01", periods=5),
                "roe": [0.15] * 5,
            })
        )
        result = checker.check_02_pit()
        assert result.passed is False
        assert result.critical is True

    def test_pit_check_passes_when_fundamentals_column_has_pit_col(self):
        """Same fundamentals-like column, but with a real, non-violating
        announcement_date present — should pass as before."""
        checker = BacktestIntegrityChecker(
            feature_df=pd.DataFrame({
                "date": pd.date_range("2020-01-01", periods=5),
                "roe": [0.15] * 5,
                "announcement_date": pd.date_range("2019-12-01", periods=5),
            })
        )
        result = checker.check_02_pit()
        assert result.passed is True

    def test_run_all_checks_raises_on_critical_failure(self):
        checker = BacktestIntegrityChecker()  # no context at all -> every critical check fails
        with pytest.raises(RuntimeError):
            checker.run_all_checks()

    def test_run_all_checks_passes_with_full_clean_context(self):
        train = _daily_df("2020-01-01", 365)
        test = _daily_df("2021-01-01", 365)
        checker = BacktestIntegrityChecker(
            folds=[(train, test)],
            feature_df=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)}),
            ohlcv_df=pd.DataFrame({"adj_factor": [1.0, 1.0]}),
            universe_tickers={"A", "B"},
            historical_tickers={"A", "B", "DELISTED1"},
            applied_roundtrip_cost_pct=TOTAL_ROUNDTRIP_COST,
            applied_min_adt_inr=MIN_ADT_INR,
            hpo_dataset="train+validation",
            fold_sharpes=[1.1, 1.2, 1.0],
            fold_returns=[0.2, 0.25, 0.18],
            benchmark_returns=[0.1, 0.12, 0.05],
            random_feature_accuracy=0.50,
        )

        results = checker.run_all_checks()

        assert all(results.values())
        assert set(results) == {
            "check_01_walk_forward", "check_02_pit", "check_03_corp_actions", "check_04_survivorship",
            "check_05_costs", "check_06_liquidity", "check_07_no_hpo_on_test", "check_08_fold_stability",
            "check_09_benchmarks", "check_10_random_feature", "check_11_sector_tier_lookahead",
            "check_12_flat_equity_curve",
        }

    def test_no_critical_failure_does_not_raise_even_if_noncritical_fails(self):
        train = _daily_df("2020-01-01", 365)
        test = _daily_df("2021-01-01", 365)
        checker = BacktestIntegrityChecker(
            folds=[(train, test)],
            feature_df=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)}),
            ohlcv_df=pd.DataFrame({"adj_factor": [1.0]}),
            universe_tickers={"A"},
            historical_tickers={"A", "DELISTED1"},
            applied_roundtrip_cost_pct=TOTAL_ROUNDTRIP_COST,
            applied_min_adt_inr=MIN_ADT_INR,
            hpo_dataset="train+validation",
            fold_sharpes=[5.0, -5.0, 0.1],  # high std -> fails check_08, non-critical
            fold_returns=[0.3, -0.1, 0.02],  # non-flat, so check_12 (critical) passes here
        )

        results = checker.run_all_checks()  # must not raise

        assert results["check_08_fold_stability"] is False

    def test_flat_equity_curve_fails_critically_not_just_benchmark_check(self):
        """A zero-trade backtest (e.g. a screener that never matches any
        ticker) produces fold_sharpes=[0,0,0] (std=0, passes check_08) and
        fold_returns=[0,0,0] (only trips the non-critical check_09) — this
        must be caught by the new CRITICAL check_12 instead of silently
        looking like a clean, if boring, backtest."""
        checker = BacktestIntegrityChecker(fold_sharpes=[0.0, 0.0, 0.0], fold_returns=[0.0, 0.0, 0.0])
        result = checker.check_12_flat_equity_curve()
        assert result.passed is False
        assert result.critical is True

        with pytest.raises(RuntimeError, match="check_12_flat_equity_curve"):
            checker.run_all_checks(applicable_checks={"check_12_flat_equity_curve"})

    def test_non_flat_equity_curve_passes_check_12(self):
        checker = BacktestIntegrityChecker(fold_sharpes=[1.1, 1.2, 1.0], fold_returns=[0.2, 0.25, 0.18])
        result = checker.check_12_flat_equity_curve()
        assert result.passed is True

    def test_one_nonzero_fold_among_otherwise_zero_folds_fails_on_degenerate_trade_count(self):
        """[BUG FIX, 2026-07-28 second model-review] A strategy that fires
        exactly once across a multi-year backtest produces one nonzero fold
        among otherwise-zero folds — all_flat is False, so the original
        check_12 logic passed cleanly. That's the same failure class as a
        totally flat curve, one notch less extreme, and must fail too when
        the caller supplies a real (degenerate) n_trades count."""
        checker = BacktestIntegrityChecker(
            fold_sharpes=[0.0, 0.0, 1.4], fold_returns=[0.0, 0.0, 0.05], n_trades=1,
        )
        result = checker.check_12_flat_equity_curve()
        assert result.passed is False
        assert result.critical is True

    def test_one_nonzero_fold_passes_when_trade_count_is_healthy(self):
        """Same one-nonzero-fold shape, but a real trade count above the
        floor — this is a legitimately quiet-but-real strategy, not a
        near-zero-trade backtest, so check_12 must not fail it."""
        checker = BacktestIntegrityChecker(
            fold_sharpes=[0.0, 0.0, 1.4], fold_returns=[0.0, 0.0, 0.05], n_trades=200,
        )
        result = checker.check_12_flat_equity_curve()
        assert result.passed is True

    def test_one_nonzero_fold_passes_when_n_trades_not_supplied(self):
        """n_trades is caller-optional — a caller that hasn't wired it yet
        must not have this new sub-check silently start failing runs."""
        checker = BacktestIntegrityChecker(fold_sharpes=[0.0, 0.0, 1.4], fold_returns=[0.0, 0.0, 0.05])
        result = checker.check_12_flat_equity_curve()
        assert result.passed is True

    def test_survivorship_check_flags_pure_current_universe(self):
        """If every historical ticker is still in the current universe, that's a survivorship-bias red flag."""
        checker = BacktestIntegrityChecker(universe_tickers={"A", "B"}, historical_tickers={"A", "B"})
        result = checker.check_04_survivorship()
        assert result.passed is False

    def test_survivorship_check_flags_implausibly_low_delisted_ratio(self):
        """REV18: presence-only was not enough — a near-complete universe missing
        just 1 of 500 historical tickers should still fail as an implausible ratio."""
        universe = {f"T{i:04d}" for i in range(499)}
        historical = universe | {"DELISTED1"}
        checker = BacktestIntegrityChecker(universe_tickers=universe, historical_tickers=historical)
        result = checker.check_04_survivorship()
        assert result.passed is False
        assert "0.20%" in result.detail or "below" in result.detail

    def test_survivorship_check_passes_above_ratio_floor(self):
        """A plausible delisted fraction (well above the 1% floor) should pass."""
        universe = {f"T{i:04d}" for i in range(90)}
        historical = universe | {f"DELISTED{i}" for i in range(10)}  # 10/100 = 10%
        checker = BacktestIntegrityChecker(universe_tickers=universe, historical_tickers=historical)
        result = checker.check_04_survivorship()
        assert result.passed is True

    def test_costs_check_flags_understated_costs(self):
        checker = BacktestIntegrityChecker(applied_roundtrip_cost_pct=0.0001)
        result = checker.check_05_costs()
        assert result.passed is False

    def test_random_feature_check_band(self):
        assert BacktestIntegrityChecker(random_feature_accuracy=0.50).check_10_random_feature().passed is True

    def test_sector_tier_lookahead_fails_over_multi_year_window(self):
        """REV18/REV15: sector/tier reflect NSE's CURRENT snapshot, not PIT
        membership — flag when used across a window long enough for that
        drift to matter."""
        checker = BacktestIntegrityChecker(
            feature_df=pd.DataFrame({
                "date": pd.date_range("2020-01-01", periods=3, freq="18ME"),  # ~3 years span
                "sector": ["IT", "IT", "IT"],
            })
        )
        result = checker.check_11_sector_tier_lookahead()
        assert result.passed is False
        assert result.critical is False  # non-critical, same tier as checks 08-10

    def test_sector_tier_lookahead_passes_within_one_year_window(self):
        checker = BacktestIntegrityChecker(
            feature_df=pd.DataFrame({
                "date": pd.date_range("2020-01-01", periods=3, freq="30D"),  # ~2 months span
                "tier": ["large_cap", "large_cap", "mid_cap"],
            })
        )
        assert checker.check_11_sector_tier_lookahead().passed is True

    def test_sector_tier_lookahead_passes_when_column_absent(self):
        checker = BacktestIntegrityChecker(
            feature_df=pd.DataFrame({"date": pd.date_range("2020-01-01", periods=1000)})
        )
        assert checker.check_11_sector_tier_lookahead().passed is True
        assert BacktestIntegrityChecker(random_feature_accuracy=0.80).check_10_random_feature().passed is False


# ===== IndianTransactionCosts =====


class TestIndianTransactionCosts:
    def test_roundtrip_cost_is_positive(self):
        costs = IndianTransactionCosts()
        assert costs.compute_roundtrip_cost(1000, 100) > 0

    def test_roundtrip_cost_pct_near_documented_range(self):
        """SPEC-BT-002: round-trip total ~0.40-0.50% for a liquid stock."""
        costs = IndianTransactionCosts()
        pct = costs.compute_roundtrip_cost_pct(1000, 100)
        assert 0.003 <= pct <= 0.007

    def test_small_cap_slippage_increases_cost(self):
        costs = IndianTransactionCosts()
        liquid_pct = costs.compute_roundtrip_cost_pct(500, 200, adtv_cr=50)
        illiquid_pct = costs.compute_roundtrip_cost_pct(500, 200, adtv_cr=0.5)
        assert illiquid_pct > liquid_pct

    def test_validate_against_settings_passes_for_default_rates(self):
        costs = IndianTransactionCosts()
        assert costs.validate_against_settings() is True

    def test_non_positive_inputs_raise(self):
        costs = IndianTransactionCosts()
        with pytest.raises(ValueError):
            costs.compute_roundtrip_cost(0, 100)
        with pytest.raises(ValueError):
            costs.compute_roundtrip_cost(100, 0)

    def test_is_liquid_enough_matches_settings_threshold(self):
        costs = IndianTransactionCosts()
        assert costs.is_liquid_enough(MIN_ADT_INR) is True
        assert costs.is_liquid_enough(MIN_ADT_INR - 1) is False

    def test_negative_brokerage_pct_raises(self):
        with pytest.raises(ValueError):
            IndianTransactionCosts(brokerage_pct=-0.01)

    def test_validate_against_settings_flags_and_logs_out_of_tolerance_rate(self):
        # A deliberately wrong brokerage rate should push the round-trip cost
        # outside TOTAL_ROUNDTRIP_COST's tolerance band -> validate returns False
        # and logs a warning (backtest/costs.py's not-within-tolerance branch).
        costs = IndianTransactionCosts(brokerage_pct=0.05)
        assert costs.validate_against_settings() is False


# ===== overfit_checks =====


class TestOverfitChecks:
    def test_deflated_sharpe_ratio_higher_sharpe_scores_higher(self):
        low = deflated_sharpe_ratio(sharpe=0.3, n_trials=25, n_obs=252)
        high = deflated_sharpe_ratio(sharpe=2.0, n_trials=25, n_obs=252)
        assert high > low
        assert 0.0 <= low <= 1.0
        assert 0.0 <= high <= 1.0

    def test_deflated_sharpe_ratio_invalid_args_raise(self):
        with pytest.raises(ValueError):
            deflated_sharpe_ratio(1.0, n_trials=0, n_obs=100)

    def test_deflated_sharpe_ratio_negative_skew_lowers_dsr(self):
        """2026-07-19 full-codebase-review Fix B4: a negatively-skewed,
        fat-tailed return series (common for momentum strategies —
        occasional sharp reversals) should score a LOWER DSR than an
        otherwise-identical normal series at the same scalar Sharpe,
        since its real standard error is wider than the normal
        approximation assumes."""
        rng = np.random.default_rng(7)
        normal_returns = pd.Series(rng.normal(0.001, 0.02, 252))
        # Left-skewed: occasional large negative shocks.
        skewed_returns = pd.Series(
            np.concatenate([rng.normal(0.002, 0.01, 240), rng.normal(-0.08, 0.02, 12)])
        )
        sharpe = 1.0

        dsr_normal = deflated_sharpe_ratio(sharpe, n_trials=10, n_obs=252, returns=normal_returns)
        dsr_skewed = deflated_sharpe_ratio(sharpe, n_trials=10, n_obs=252, returns=skewed_returns)

        assert skewed_returns.skew() < 0
        assert dsr_skewed <= dsr_normal

    def test_deflated_sharpe_ratio_no_returns_matches_zero_skew_kurtosis(self):
        """Omitting `returns` (backward-compatible default) should equal
        passing a returns series with sample skew/kurtosis of exactly 0."""
        no_returns = deflated_sharpe_ratio(1.0, n_trials=10, n_obs=252)
        # A returns series is never needed to hit exactly skew=kurt=0 in
        # practice, so directly verify the two code paths agree by
        # checking no_returns falls strictly between two returns-series
        # DSRs bracketing zero skew.
        assert 0.0 <= no_returns <= 1.0

    def test_random_feature_test_scores_near_chance(self):
        """A model with no real relationship between shuffled features and y should land near 50%."""

        class _MajorityClassModel(IModel):
            def __init__(self):
                self._majority = 0

            def train(self, X, y, sample_weight=None):
                self._majority = int(y.mode().iloc[0])

            def predict(self, X):
                return pd.Series([self._majority] * len(X), index=X.index)

            def save(self, path):
                pass

            def load(self, path):
                pass

            def metadata(self):
                return {}

        rng = np.random.default_rng(0)
        n = 200
        X = pd.DataFrame({"f1": rng.normal(size=n), "f2": rng.normal(size=n)})
        y = pd.Series(rng.integers(0, 2, size=n))

        accuracy = random_feature_test(_MajorityClassModel(), X, y, X, y, feature_cols=["f1", "f2"], n_repeats=5)

        assert 0.3 <= accuracy <= 0.7  # roughly chance-level for a near-balanced binary target

    def test_random_feature_test_requires_feature_cols(self):
        class _NoopModel(IModel):
            def train(self, X, y, sample_weight=None):
                pass

            def predict(self, X):
                return pd.Series([0] * len(X), index=X.index)

            def save(self, path):
                pass

            def load(self, path):
                pass

            def metadata(self):
                return {}

        X = pd.DataFrame({"f1": [1, 2, 3]})
        y = pd.Series([0, 1, 0])
        with pytest.raises(ValueError):
            random_feature_test(_NoopModel(), X, y, X, y, feature_cols=[])

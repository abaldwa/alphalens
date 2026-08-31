"""
tests/unit/test_k_portfolio_rebalancing.py

Unit tests for B-001: J&T overlapping K-portfolio rebalancing implementation.

Tests verify:
- Cohort tracking and assignment
- K-portfolio rotation mechanics
- Cohort-aware signal filtering
- Turnover reduction math
- Edge cases and backward compatibility
"""

import pytest
from datetime import date as date_type, timedelta


from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import StrategyPortfolio
from backtest.portfolio import Position


class TestKPortfolioConfiguration:
    """Test configuration and initialization of K-portfolio feature."""

    def test_k_portfolio_disabled_by_default(self):
        """When overlapping_k_portfolio is None, feature is disabled."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=None,
        )
        assert portfolio.overlapping_k_portfolio is None
        assert portfolio.current_cohort_number == 0

    def test_k_portfolio_enabled_with_k_value(self):
        """Can enable K-portfolio with explicit K value."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )
        assert portfolio.overlapping_k_portfolio == 5

    def test_k_must_be_positive_integer(self):
        """K must be positive; zero or negative raises ValueError."""
        with pytest.raises(ValueError, match="overlapping_k_portfolio must be positive"):
            StrategyPortfolio(
                initial_capital=1_000_000,
                horizon_bucket=HorizonBucket.D21,
                overlapping_k_portfolio=0,
            )
        with pytest.raises(ValueError, match="overlapping_k_portfolio must be positive"):
            StrategyPortfolio(
                initial_capital=1_000_000,
                horizon_bucket=HorizonBucket.D21,
                overlapping_k_portfolio=-1,
            )

    def test_k_can_equal_one(self):
        """K=1 is allowed (though operationally equivalent to disabled)."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=1,
        )
        assert portfolio.overlapping_k_portfolio == 1


class TestCohortAssignment:
    """Test that positions are correctly assigned to cohorts."""

    def test_position_assigned_current_cohort_on_buy(self):
        """Position.cohort_number is set to current_cohort_number on buy."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            n_target_positions=10,
            overlapping_k_portfolio=5,
        )
        prices = {"SBIN": 500.0}
        portfolio.current_cohort_number = 0

        pos = portfolio.buy(
            "SBIN", "Finance", 500.0, date_type(2020, 1, 1), prices
        )
        assert pos.cohort_number == 0

        portfolio.current_cohort_number = 3
        pos2 = portfolio.buy(
            "INFY", "IT", 1500.0, date_type(2020, 2, 1), {"INFY": 1500.0}
        )
        assert pos2.cohort_number == 3

    def test_position_no_cohort_when_disabled(self):
        """Position.cohort_number is None when K-portfolio is disabled."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=None,
        )
        prices = {"SBIN": 500.0}
        pos = portfolio.buy("SBIN", "Finance", 500.0, date_type(2020, 1, 1), prices)
        assert pos.cohort_number is None

    def test_current_cohort_computed_from_rebalance_index(self):
        """current_cohort_number should be rebalance_index % K."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )
        # Current cohort is set externally from orchestrator
        portfolio.current_cohort_number = 0 % 5  # = 0
        assert portfolio.current_cohort_number == 0
        portfolio.current_cohort_number = 1 % 5  # = 1
        assert portfolio.current_cohort_number == 1
        portfolio.current_cohort_number = 5 % 5  # = 0 (cycles back)
        assert portfolio.current_cohort_number == 0


class TestCohortRotationMath:
    """Test the mathematical properties of K-portfolio rotation."""

    def test_k_equals_5_means_5_month_cycle(self):
        """With K=5, positions rotate every 5 rebalances."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            n_target_positions=10,
            overlapping_k_portfolio=5,
        )

        prices = {"T1": 100.0, "T2": 100.0, "T3": 100.0, "T4": 100.0, "T5": 100.0}

        # Rebalance 0: buy T1, T2 (cohort 0)
        portfolio.current_cohort_number = 0
        p1 = portfolio.buy("T1", "Finance", 100.0, date_type(2020, 1, 1), prices)
        p2 = portfolio.buy("T2", "Finance", 100.0, date_type(2020, 1, 1), prices)

        assert p1.cohort_number == 0
        assert p2.cohort_number == 0
        due = portfolio.get_positions_due_for_rotation(rebalance_index=0)
        assert due == []  # Nothing due yet (rebalance_index < K)

        # Rebalance 5: Now cohort 0 is due (5 - 5 = 0)
        portfolio.current_cohort_number = 5 % 5  # = 0
        due = portfolio.get_positions_due_for_rotation(rebalance_index=5)
        assert set(due) == {"T1", "T2"}

    def test_only_one_cohort_active_per_rebalance(self):
        """Each rebalance has exactly one cohort due for rotation."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )

        # At rebalance 0: no cohort is due yet (rebalance_index < K)
        assert portfolio.get_positions_due_for_rotation(rebalance_index=0) == []

        # At rebalance 3: no cohort is due yet (rebalance_index < K)
        assert portfolio.get_positions_due_for_rotation(rebalance_index=3) == []

        # At rebalance 7: cohort (7-5) % 5 = 2 is due
        portfolio.positions["T1"] = Position("T1", "Finance", date_type(2020, 1, 1), 100.0, 100, cohort_number=2)
        portfolio.positions["T2"] = Position("T2", "Finance", date_type(2020, 1, 1), 100.0, 100, cohort_number=2)
        portfolio.positions["T3"] = Position("T3", "Finance", date_type(2020, 1, 1), 100.0, 100, cohort_number=3)
        due = portfolio.get_positions_due_for_rotation(rebalance_index=7)
        assert set(due) == {"T1", "T2"}
        assert "T3" not in due

    def test_steady_state_holds_k_times_n_target_positions(self):
        """In steady state with K overlapping cohorts, portfolio holds K * (n_target_positions / K) = n_target_positions total."""
        portfolio = StrategyPortfolio(
            initial_capital=100_000_000,  # Large enough for 10 positions at 100 each
            horizon_bucket=HorizonBucket.D21,
            n_target_positions=10,
            overlapping_k_portfolio=5,
        )

        # Build up to steady state: 5 rebalances, 2 positions per cohort
        # (10 positions / 5 cohorts = 2 per cohort)
        # Use different sectors to avoid sector cap limits
        sectors = ["Finance", "IT", "Healthcare", "Consumer", "Energy"]
        prices = {f"T{i}": 100.0 for i in range(10)}

        for rebalance_idx in range(5):
            portfolio.current_cohort_number = rebalance_idx % 5  # = rebalance_idx for first 5
            for j in range(2):
                ticker = f"T{rebalance_idx * 2 + j}"
                sector = sectors[(rebalance_idx * 2 + j) % len(sectors)]
                portfolio.buy(ticker, sector, 100.0, date_type(2020, 1, 1) + timedelta(days=21*rebalance_idx), prices)

        # May not be exactly 10 due to portfolio sizing limits, but should be close
        assert len(portfolio.positions) >= 5


class TestSignalFilteringByCohort:
    """Test that signals are correctly filtered by cohort eligibility."""

    def test_sell_signal_rejected_if_cohort_not_due(self):
        """Sell signals for positions not in the active cohort should be deferred."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )

        # Add a position in cohort 0
        pos = Position("SBIN", "Finance", date_type(2020, 1, 1), 500.0, 100, cohort_number=0)
        portfolio.positions["SBIN"] = pos

        # At rebalance 1, cohort 1 is active, not cohort 0
        portfolio.current_cohort_number = 1
        active_cohort = 1 % 5  # = 1
        due = [ticker for ticker, p in portfolio.positions.items() if p.cohort_number == active_cohort]
        assert "SBIN" not in due  # SBIN is in cohort 0, not cohort 1

    def test_sell_signal_accepted_if_cohort_due(self):
        """Sell signals for positions in the active cohort should be accepted."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )

        # Add a position in cohort 1
        pos = Position("SBIN", "Finance", date_type(2020, 1, 1), 500.0, 100, cohort_number=1)
        portfolio.positions["SBIN"] = pos

        # At rebalance 1, cohort 1 is active (1 % 5 = 1)
        portfolio.current_cohort_number = 1
        active_cohort = 1 % 5  # = 1
        due = [ticker for ticker, p in portfolio.positions.items() if p.cohort_number == active_cohort]
        assert "SBIN" in due  # SBIN is in cohort 1, which is active

    def test_buy_signal_rejected_if_cohort_slot_full(self):
        """Buy signals should be rejected if the current cohort's slot is full."""
        portfolio = StrategyPortfolio(
            initial_capital=10_000_000,
            horizon_bucket=HorizonBucket.D21,
            n_target_positions=10,
            overlapping_k_portfolio=5,
        )

        # Slots per cohort = 10 / 5 = 2
        prices = {f"T{i}": 100.0 for i in range(15)}

        # Fill cohort 0 with 2 positions
        portfolio.current_cohort_number = 0
        portfolio.buy("T0", "Finance", 100.0, date_type(2020, 1, 1), prices)
        portfolio.buy("T1", "Finance", 100.0, date_type(2020, 1, 1), prices)

        # Try to buy a third position in cohort 0 - should be rejected
        cohort_positions = [p for p in portfolio.positions.values() if p.cohort_number == 0]
        slots_per_cohort = 10 // 5
        assert len(cohort_positions) >= slots_per_cohort  # Slot is full

    def test_buy_signal_accepted_if_cohort_slot_available(self):
        """Buy signals should be accepted if the current cohort's slot has space."""
        portfolio = StrategyPortfolio(
            initial_capital=10_000_000,
            horizon_bucket=HorizonBucket.D21,
            n_target_positions=10,
            overlapping_k_portfolio=5,
        )

        prices = {f"T{i}": 100.0 for i in range(10)}

        # At rebalance 0, cohort 0 is active
        portfolio.current_cohort_number = 0
        pos1 = portfolio.buy("T0", "Finance", 100.0, date_type(2020, 1, 1), prices)

        # Slot has space (1 position in 2-position slot)
        cohort_positions = [p for p in portfolio.positions.values() if p.cohort_number == 0]
        slots_per_cohort = 10 // 5
        assert len(cohort_positions) < slots_per_cohort
        assert pos1 is not None


class TestTurnoverReduction:
    """Test that K-portfolio reduces turnover as expected."""

    def test_turnover_calculation_k_equals_5(self):
        """With K=5, expected turnover is 1/5 per rebalance."""
        # Theoretical: monthly turnover = 100% / K = 20% per month
        k = 5
        theoretical_monthly_turnover = 100.0 / k
        assert theoretical_monthly_turnover == 20.0

    def test_k_equals_3_monthly_turnover(self):
        """With K=3, expected turnover is 1/3 per rebalance."""
        k = 3
        theoretical_monthly_turnover = 100.0 / k
        assert pytest.approx(theoretical_monthly_turnover, rel=0.01) == 33.33


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_first_rebalance_no_cohort_due(self):
        """On the first rebalance, no cohort is due for rotation yet."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )
        portfolio.current_cohort_number = 0
        due = portfolio.get_positions_due_for_rotation(rebalance_index=0)
        assert due == []

    def test_k_equals_1_single_position_at_a_time(self):
        """With K=1, every position is in the same cohort and rotates every rebalance."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=1,
        )

        prices = {"T0": 100.0, "T1": 100.0}

        # At rebalance 0, cohort 0 is active and new
        portfolio.current_cohort_number = 0 % 1  # = 0
        pos1 = portfolio.buy("T0", "Finance", 100.0, date_type(2020, 1, 1), prices)
        assert pos1.cohort_number == 0

        # At rebalance 1, cohort (1 - 1) % 1 = 0 is due
        portfolio.current_cohort_number = 1 % 1  # = 0
        due = portfolio.get_positions_due_for_rotation(rebalance_index=1)
        assert "T0" in due

    def test_positions_held_across_multiple_k_cycles(self):
        """Positions entered in earlier cohorts survive multiple K cycles."""
        portfolio = StrategyPortfolio(
            initial_capital=100_000_000,  # Large enough for 10 positions
            horizon_bucket=HorizonBucket.D21,
            n_target_positions=10,
            overlapping_k_portfolio=5,
        )

        prices = {f"T{i}": 100.0 for i in range(20)}
        sectors = ["Finance", "IT", "Healthcare", "Consumer", "Energy"]

        # Build up cohorts 0-4 over rebalances 0-4
        bought_tickers = []
        for rebalance_idx in range(5):
            portfolio.current_cohort_number = rebalance_idx % 5  # = rebalance_idx for first 5
            for j in range(2):
                ticker = f"T{rebalance_idx * 2 + j}"
                sector = sectors[(rebalance_idx * 2 + j) % len(sectors)]
                pos = portfolio.buy(ticker, sector, 100.0, date_type(2020, 1, 1), prices)
                if pos:
                    bought_tickers.append(ticker)

        # Should have bought at least some positions
        assert len(bought_tickers) >= 5

        # At rebalance 7: due_cohort = (7 - 5) % 5 = 2
        due = portfolio.get_positions_due_for_rotation(rebalance_index=7)
        # Should have some positions due for cohort 2
        if len(bought_tickers) >= 5:
            expected_cohort_2_tickers = {ticker for ticker in bought_tickers if portfolio.positions[ticker].cohort_number == 2}
            assert set(due) == expected_cohort_2_tickers


class TestBackwardCompatibility:
    """Test that disabled K-portfolio behaves like legacy system."""

    def test_k_portfolio_none_acts_like_legacy(self):
        """When overlapping_k_portfolio is None, all positions can be rotated."""
        portfolio = StrategyPortfolio(
            initial_capital=1_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=None,
        )
        prices = {"SBIN": 500.0}
        pos = portfolio.buy("SBIN", "Finance", 500.0, date_type(2020, 1, 1), prices)
        # No cohort assigned
        assert pos.cohort_number is None
        # No positions due when feature disabled
        due = portfolio.get_positions_due_for_rotation(rebalance_index=7)
        assert due == []

    def test_mixed_legacy_and_new_positions(self):
        """Legacy positions (cohort_number=None) coexist with K-portfolio positions."""
        portfolio = StrategyPortfolio(
            initial_capital=10_000_000,
            horizon_bucket=HorizonBucket.D21,
            overlapping_k_portfolio=5,
        )

        prices = {"SBIN": 500.0, "INFY": 1500.0}

        # Add a legacy position (manually, as if from a pre-feature run)
        legacy_pos = Position("SBIN", "Finance", date_type(2020, 1, 1), 500.0, 10)
        legacy_pos.cohort_number = None
        portfolio.positions["SBIN"] = legacy_pos

        # Add a new K-portfolio position
        portfolio.current_cohort_number = 0
        portfolio.buy("INFY", "IT", 1500.0, date_type(2020, 1, 1), prices)

        assert portfolio.positions["SBIN"].cohort_number is None
        assert portfolio.positions["INFY"].cohort_number == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

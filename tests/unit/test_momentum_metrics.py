"""tests/unit/test_momentum_metrics.py — ML38 backtest/momentum_metrics.py."""

import pytest

from backtest.momentum_metrics import cagr, churn_factor, total_return, xirr


class TestXirr:
    def test_matches_cagr_for_a_single_lump_sum(self):
        # A single contribution + a single payout is just CAGR in disguise.
        r = 0.15
        starting = 1_000_000
        ending = starting * (1 + r) ** 5
        result = xirr([("2016-01-01", -starting), ("2021-01-01", ending)])
        assert result == pytest.approx(r, abs=1e-3)

    def test_monthly_contributions_solved_rate_zeroes_the_npv(self):
        # A monthly SIP has no simple closed-form expected rate (day-count
        # vs. calendar-month compounding don't line up cleanly) — instead
        # verify the solved rate is a genuine root: plugging it back into
        # the same actual/365 NPV formula xirr() itself uses should zero out.
        import pandas as pd

        contribution = 50_000
        flows = [(f"2020-{m+1:02d}-01", -contribution) for m in range(12)]
        flows.append(("2021-01-15", 700_000))  # some plausible ending value

        result = xirr(flows)
        assert result is not None

        dates = [pd.Timestamp(d) for d, _ in flows]
        amounts = [a for _, a in flows]
        anchor = min(dates)
        npv = sum(a / (1.0 + result) ** ((d - anchor).days / 365.0) for d, a in zip(dates, amounts))
        assert npv == pytest.approx(0.0, abs=1.0)

    def test_returns_none_when_unbracketable(self):
        # All-negative cash flows: no rate makes NPV cross zero.
        assert xirr([("2020-01-01", -100), ("2020-06-01", -50)]) is None

    def test_rejects_fewer_than_two_flows(self):
        with pytest.raises(ValueError):
            xirr([("2020-01-01", -100)])


class TestTotalReturn:
    def test_positive_return(self):
        assert total_return(1_000_000, 1_500_000) == pytest.approx(0.5)

    def test_rejects_non_positive_starting_capital(self):
        with pytest.raises(ValueError):
            total_return(0, 100)


class TestCagr:
    def test_doubles_in_10_years(self):
        # exact doubling in 10 years is close to but not exactly 2**(1/10)-1;
        # use round-trip: pick ending_value = starting * (1+r)^10 for a known r.
        r = 0.10
        starting = 1_000_000
        ending = starting * (1 + r) ** 10
        result = cagr(starting, ending, "2016-01-01", "2026-01-01")
        assert result == pytest.approx(r, abs=1e-3)

    def test_rejects_bad_dates(self):
        with pytest.raises(ValueError):
            cagr(1_000_000, 1_100_000, "2026-01-01", "2020-01-01")


class TestChurnFactor:
    def test_per_rebalance_and_annual_average(self):
        events = [
            {"date": "2020-01-06", "n_bought": 2, "n_sold": 1},
            {"date": "2020-06-06", "n_bought": 1, "n_sold": 1},
            {"date": "2021-01-04", "n_bought": 3, "n_sold": 2},
        ]
        result = churn_factor(events)
        assert result["per_rebalance"][0]["n_transactions"] == 3
        # 2020: (2+1)+(1+1)=5 transactions; 2021: (3+2)=5 transactions -> avg 5.0
        assert result["avg_transactions_per_year"] == pytest.approx(5.0)

    def test_empty_events(self):
        result = churn_factor([])
        assert result == {"per_rebalance": [], "avg_transactions_per_year": 0.0}

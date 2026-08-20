"""
Tests for backtest/ta_comparison_report.py — the FY and rolling-return maths
behind the Technical comparison dashboard.

These numbers go straight onto a screen people make decisions from, and both
calculations have a quiet failure mode: an FY series that does not compound back
to the total return, and a rolling window that silently uses the wrong endpoint
when a boundary is not a trading day. Both are pinned here.
"""

from datetime import date

import pytest

from backtest.ta_comparison_report import _fy_end_of, _template_of, fy_returns, rolling_returns


def _curve(points):
    return [{"date": d, "equity": e} for d, e in points]


class TestTemplateOf:
    def test_extracts_template_from_strategy_id(self):
        assert _template_of("ta_c6_63d_20260812") == "C6"
        assert _template_of("ta_s001_21d_20260812") == "S001"

    def test_degrades_without_raising(self):
        # Keying the whole dashboard off this, so it must never explode.
        assert _template_of("") == "?"
        assert _template_of("weird") == "weird"


class TestFinancialYearEnd:
    def test_april_starts_the_new_fy(self):
        assert _fy_end_of(date(2020, 4, 1)) == date(2021, 3, 31)
        assert _fy_end_of(date(2020, 3, 31)) == date(2020, 3, 31)


class TestFyReturns:
    def test_returns_compound_back_to_the_total(self):
        """The property that matters: chaining the yearly returns must reproduce
        the full-period return. If each FY were measured from its own first
        observation instead of the previous close, the gap across the boundary
        would be dropped and this would drift."""
        curve = _curve([
            ("2020-04-01", 1_000_000.0), ("2021-03-31", 1_200_000.0),
            ("2021-04-01", 1_200_000.0), ("2022-03-31", 1_500_000.0),
            ("2022-04-01", 1_500_000.0), ("2023-03-31", 1_350_000.0),
        ])
        rows = fy_returns(curve)
        # Labelled by FY-ENDING year, matching core/metrics.py. Both channels
        # must agree: the report grid keys its year columns on this string, so
        # two conventions produced two columns for the same financial year.
        assert [r["fy_label"] for r in rows] == ["FY2021", "FY2022", "FY2023"]
        assert rows[0]["return_pct"] == pytest.approx(20.0)
        assert rows[1]["return_pct"] == pytest.approx(25.0)
        assert rows[2]["return_pct"] == pytest.approx(-10.0)

        compounded = 1.0
        for r in rows:
            compounded *= 1 + r["return_pct"] / 100
        assert compounded == pytest.approx(1_350_000.0 / 1_000_000.0)

    def test_each_year_opens_where_the_last_one_closed(self):
        """A gap across the FY boundary (holidays) must not be silently dropped."""
        curve = _curve([
            ("2020-04-01", 1_000_000.0), ("2021-03-31", 1_200_000.0),
            ("2021-04-05", 1_260_000.0), ("2022-03-31", 1_400_000.0),
        ])
        rows = fy_returns(curve)
        assert rows[1]["opening_equity"] == pytest.approx(1_200_000.0)
        assert rows[1]["return_pct"] == pytest.approx((1_400_000 / 1_200_000 - 1) * 100)

    def test_stub_first_year_is_flagged_not_annualised(self):
        """Annualising a 2-month stub would inflate it; dropping it would hide
        deployed capital. It is reported as-is and flagged."""
        rows = fy_returns(_curve([("2020-02-01", 1_000_000.0), ("2020-03-31", 1_100_000.0)]))
        assert rows[0]["partial"] is True
        assert rows[0]["return_pct"] == pytest.approx(10.0)

    def test_full_first_year_is_not_flagged_partial(self):
        rows = fy_returns(_curve([("2020-04-01", 1_000_000.0), ("2021-03-31", 1_100_000.0)]))
        assert rows[0]["partial"] is False

    def test_empty_curve(self):
        assert fy_returns([]) == []


class TestRollingReturns:
    def test_annualises_over_the_window(self):
        # Exactly 2x over 2 years -> sqrt(2) - 1 = 41.42% annualised.
        curve = _curve([("2020-04-01", 1_000_000.0), ("2022-03-31", 2_000_000.0)])
        out = rolling_returns(curve, windows_years=(2,))
        assert out["2y"]["n_windows"] == 1
        assert out["2y"]["median_pct"] == pytest.approx(41.42, abs=0.01)

    def test_window_longer_than_the_data_yields_nothing(self):
        curve = _curve([("2020-04-01", 1_000_000.0), ("2021-03-31", 1_100_000.0)])
        assert rolling_returns(curve, windows_years=(5,)) == {}

    def test_uses_the_last_point_on_or_before_a_non_trading_boundary(self):
        """1 April is frequently a holiday. The window must fall back to the
        prior close rather than skipping the window or reaching forward."""
        curve = _curve([
            ("2020-03-30", 1_000_000.0),   # 1 Apr 2020 itself absent
            ("2022-03-30", 2_000_000.0), ("2022-03-31", 2_000_000.0),
        ])
        out = rolling_returns(curve, windows_years=(2,))
        assert out["2y"]["n_windows"] == 1
        assert out["2y"]["median_pct"] == pytest.approx(41.42, abs=0.01)

    def test_reports_a_distribution_not_a_single_number(self):
        # Multipliers must not be periodic: a strict 1.5/0.9 alternation makes
        # every 2-year window exactly 1.35 and worst == median, which fails this
        # assertion for a reason that has nothing to do with the code.
        pts = [("2015-04-01", 1_000_000.0)]
        equity = 1_000_000.0
        for year, mult in zip(range(2016, 2024), (1.5, 0.9, 1.1, 0.7, 2.0, 1.05, 0.85, 1.3)):
            equity *= mult
            pts.append((f"{year}-03-31", equity))
        out = rolling_returns(_curve(pts), windows_years=(2,))
        r = out["2y"]
        assert r["n_windows"] > 1
        assert r["worst_pct"] < r["median_pct"] < r["best_pct"]
        assert 0 <= r["positive_windows"] <= r["n_windows"]

    def test_empty_curve(self):
        assert rolling_returns([]) == {}

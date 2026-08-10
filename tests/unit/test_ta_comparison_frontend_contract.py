"""
tests/unit/test_ta_comparison_frontend_contract.py

Guards the field-name contract between backtest/ta_comprehensive_metrics.py
and frontend/src/pages/backtest/TaComparisonPanel.tsx.

[2026-08-10] Written after the panel was built against ASSUMED field names and
silently rendered "—" for six columns: it read `n_trades`, `avg_positions_held`,
`signals_per_month` and `yearly[].return_on_capital`, none of which exist. The
real names are `closed_trades`, `holdings.avg_concurrent_positions_calendar`,
`entries.avg_entries_per_month` and `yearly[].return_pct`.

A schema drift here produces no error anywhere — the UI just shows dashes — so
it has to be asserted explicitly.
"""

import pandas as pd
import pytest

from backtest.ta_comprehensive_metrics import compute_comprehensive_metrics


# Exactly the real trade-book header, taken from a live
# backtest/reports/trade_log_*.csv — pnl_pct in particular is required by
# exit_reason_breakdown() and is easy to omit when hand-rolling a fixture.
TRADE_COLUMNS = [
    "ticker", "qty", "buy_date", "buy_price", "sale_date", "sale_price",
    "stock_rank", "pnl_inr", "pnl_pct", "exit_reason",
]


def _trade(buy, sale, pnl, ticker="AAA"):
    qty = 10
    buy_price = 100.0
    return {
        "ticker": ticker,
        "qty": qty,
        "buy_date": buy,
        "buy_price": buy_price,
        "sale_date": sale,
        "sale_price": buy_price + pnl / qty,
        "stock_rank": 1,
        "pnl_inr": pnl,
        "pnl_pct": pnl / (qty * buy_price),
        "exit_reason": "signal",
    }


@pytest.fixture
def report(tmp_path):
    """A parsed orchestrator_*.json plus its side-car trade log.

    Mirrors the real shape: run details are NESTED under `run` (not
    top-level), and trades live in the CSV at `trade_log_path` rather than
    inside the JSON.
    """
    trade_log = tmp_path / "trades.csv"
    pd.DataFrame(
        [
            _trade("2007-05-02", "2007-06-02", 5000.0),
            _trade("2008-05-02", "2009-08-02", 25000.0),  # held >1y -> LTCG
            _trade("2009-05-02", "2009-06-02", -3000.0, ticker="BBB"),
        ],
        columns=TRADE_COLUMNS,
    ).to_csv(trade_log, index=False)

    dates = pd.bdate_range("2007-04-02", "2010-04-02")
    return {
        "trade_log_path": str(trade_log),
        "run": {
            "run_id": "r1",
            "strategy_id": "ta_a1_21d",
            "start_date": "2007-04-02",
            "end_date": "2010-04-02",
            "initial_capital": 1_000_000.0,
            "config": {"template_name": "A1"},
        },
        "metrics": {"cagr": 0.11, "sharpe": 1.2, "max_drawdown": -0.2},
        "equity_curve": [
            {"date": d.strftime("%Y-%m-%d"), "equity": 1_000_000.0 * (1 + 0.0002 * i)}
            for i, d in enumerate(dates)
        ],
    }


class TestFrontendFieldContract:
    """Every field TaComparisonPanel.tsx reads must exist."""

    def test_top_level_fields_the_panel_reads(self, report):
        m = compute_comprehensive_metrics(report)
        for field in (
            "template_name", "start_date", "end_date", "initial_capital",
            "engine_metrics", "yearly", "rolling", "taxes",
            "holdings", "entries",
            "closed_trades", "avg_holding_days",
        ):
            assert field in m, f"TaComparisonPanel reads `{field}` but the report has no such key"

    def test_holdings_supplies_average_stocks_held(self, report):
        """User requirement: 'average stocks held'."""
        m = compute_comprehensive_metrics(report)
        assert m["holdings"]["avg_concurrent_positions_calendar"] is not None

    def test_entries_supplies_signals_per_month(self, report):
        """User requirement: 'signals per month'."""
        m = compute_comprehensive_metrics(report)
        assert m["entries"]["avg_entries_per_month"] is not None

    def test_yearly_uses_return_pct_not_return_on_capital(self, report):
        m = compute_comprehensive_metrics(report)
        row = m["yearly"][0]
        assert "return_pct" in row
        assert "return_on_capital" not in row, (
            "yearly rows use `return_pct`; `return_on_capital` is strategy-level only"
        )

    def test_yearly_uses_indian_financial_years(self, report):
        m = compute_comprehensive_metrics(report)
        assert all(y["trading_year"].startswith("FY") for y in m["yearly"])
        # A May-2007 trade belongs to FY2007-08 (Apr 1 - Mar 31), not FY2006-07.
        assert "FY2007-08" in {y["trading_year"] for y in m["yearly"]}

    def test_rolling_windows_present_when_equity_curve_supplied(self, report):
        m = compute_comprehensive_metrics(report)
        assert m["rolling"], "rolling must populate when equity_curve is present"
        assert set(m["rolling"]) >= {"2y", "3y", "4y", "5y"}
        # A window with no complete period returns only {"n_windows": 0} — the
        # 3-year fixture curve legitimately has no 4y/5y windows, and the panel
        # must tolerate that rather than expect a full stats block everywhere.
        populated = {w: v for w, v in m["rolling"].items() if v.get("n_windows")}
        assert "2y" in populated, "a 3-year curve must yield 2-year windows"
        for stats in populated.values():
            for key in ("n_windows", "min", "median", "max", "mean",
                        "positive_share", "median_annualized"):
                assert key in stats, f"panel reads rolling.{key}"

    def test_post_tax_fields_added_by_build_comparison(self, report, tmp_path, monkeypatch):
        """post_tax_pnl_inr / total_tax_inr / post_tax_return_on_capital are
        NOT emitted by compute_comprehensive_metrics — build_comparison adds
        them from the selected tax regime. The panel reads them at top level,
        so the collation layer is what must supply them."""
        import json as _json
        import backtest.ta_comparison_report as tcr

        (tmp_path / f"orchestrator_{'suffix'}_job0.json").write_text(_json.dumps(report))
        monkeypatch.setattr(tcr, "benchmark_cagr", lambda *a, **k: 0.10)
        out = tcr.build_comparison("suffix", tmp_path, "ltcg_12_5pct_1_25L")
        assert out["strategies"], f"collation produced nothing: {out.get('failed_reports')}"
        row = out["strategies"][0]
        for field in ("post_tax_pnl_inr", "total_tax_inr", "post_tax_return_on_capital"):
            assert field in row, f"TaComparisonPanel reads `{field}`"

    def test_rolling_is_absent_without_equity_curve(self, report):
        """Older runs predate equity_curve; the panel must tolerate this."""
        report.pop("equity_curve")
        m = compute_comprehensive_metrics(report)
        assert not m.get("rolling")

    def test_both_ltcg_regimes_present(self, report):
        m = compute_comprehensive_metrics(report)
        assert set(m["taxes"]) == {"ltcg_10pct_1L", "ltcg_12_5pct_1_25L"}
        for regime in m["taxes"].values():
            assert "post_tax_pnl_inr" in regime and "total_tax_inr" in regime

"""
tests/unit/test_technical_reporting.py

Covers backtest/technical_reporting.py's entry_signal_zscores() and
signal_failure_breakdown() against small synthetic Trade lists — no real
backtest run needed (per the 2026-08-01 "do not run the backtest yet"
instruction, verification here is unit-level only).
"""

from backtest.portfolio import Trade
from backtest.technical_reporting import entry_signal_zscores, signal_failure_breakdown


def _trade(ticker, pnl_pct, matched=8, total=10, score=75.0, exit_reason="signal"):
    return Trade(
        ticker=ticker, entry_date="2024-01-01", exit_date="2024-02-01",
        entry_price=100.0, exit_price=100.0 * (1 + pnl_pct), quantity=10,
        pnl_inr=1000.0 * pnl_pct, pnl_pct=pnl_pct, cost_inr=5.0, exit_reason=exit_reason,
        entry_feature_vector={
            "template_name": "A1", "matched": True, "score": score,
            "matched_conditions": matched, "total_conditions": total,
        },
    )


class TestEntrySignalZscores:
    def test_too_few_trades_returns_none_zscores(self):
        trades = [_trade("A", 0.05, score=70), _trade("B", -0.02, score=80)]
        result = entry_signal_zscores(trades)
        assert result["n_scored"] == 2
        assert all(row["entry_signal_zscore"] is None for row in result["per_trade"])

    def test_computes_zscores_for_sufficient_population(self):
        trades = [
            _trade("A", 0.05, score=50), _trade("B", 0.02, score=60),
            _trade("C", -0.02, score=70), _trade("D", -0.10, score=200),
        ]
        result = entry_signal_zscores(trades)
        zscores = [row["entry_signal_zscore"] for row in result["per_trade"]]
        assert zscores[3] > zscores[0]  # the 200-score outlier has the highest z
        assert result["n_scored"] == 4

    def test_missing_feature_vector_yields_none_score(self):
        trade = Trade(
            ticker="X", entry_date="2024-01-01", exit_date="2024-02-01",
            entry_price=100.0, exit_price=105.0, quantity=10,
            pnl_inr=50.0, pnl_pct=0.05, cost_inr=1.0, exit_reason="signal",
            entry_feature_vector=None,
        )
        result = entry_signal_zscores([trade])
        assert result["per_trade"][0]["entry_signal_score"] is None
        assert result["per_trade"][0]["entry_signal_zscore"] is None


class TestSignalFailureBreakdown:
    def test_separates_winners_and_losers(self):
        trades = [
            _trade("A", 0.05, matched=9, total=10),
            _trade("B", -0.03, matched=6, total=10),
            _trade("C", -0.08, matched=5, total=10),
        ]
        result = signal_failure_breakdown(trades)
        assert result["n_losing_trades"] == 2
        assert result["n_winning_trades"] == 1
        assert {t["ticker"] for t in result["losing_trades"]} == {"B", "C"}

    def test_losers_have_lower_matched_ratio_reflected(self):
        trades = [
            _trade("A", 0.05, matched=9, total=10),
            _trade("B", 0.02, matched=10, total=10),
            _trade("C", -0.03, matched=5, total=10),
            _trade("D", -0.08, matched=6, total=10),
        ]
        result = signal_failure_breakdown(trades)
        assert result["mean_matched_conditions_ratio_losers"] == 0.55
        assert result["mean_matched_conditions_ratio_winners"] == 0.95

    def test_no_feature_vector_still_included_but_excluded_from_ratio(self):
        trade = Trade(
            ticker="Y", entry_date="2024-01-01", exit_date="2024-02-01",
            entry_price=100.0, exit_price=90.0, quantity=10,
            pnl_inr=-100.0, pnl_pct=-0.1, cost_inr=1.0, exit_reason="signal",
            entry_feature_vector=None,
        )
        result = signal_failure_breakdown([trade])
        assert result["n_losing_trades"] == 1
        assert result["losing_trades"][0]["entry_feature_vector"] is None
        assert result["mean_matched_conditions_ratio_losers"] is None

    def test_empty_trades(self):
        result = signal_failure_breakdown([])
        assert result["n_losing_trades"] == 0
        assert result["n_winning_trades"] == 0
        assert result["losing_trades"] == []

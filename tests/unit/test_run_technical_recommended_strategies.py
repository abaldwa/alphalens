"""tests/unit/test_run_technical_recommended_strategies.py — pure
job-list construction, signal-failure CSV parsing, and report aggregation.
No real backtest execution."""

import json
from datetime import date

from scripts import run_technical_experimentation as base_mod
from scripts import run_technical_recommended_strategies as mod


class TestBuildJobs:
    def test_quick_grid_covers_singles_and_combos(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        kinds = {j["_variant_kind"] for j in jobs}
        assert kinds == {"single", "combo"}

    def test_single_jobs_have_template_name_not_combo(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        singles = [j for j in jobs if j["_variant_kind"] == "single"]
        assert all("template_name" in j and "combo_templates" not in j for j in singles)

    def test_combo_jobs_have_combo_templates_not_template_name(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        combos = [j for j in jobs if j["_variant_kind"] == "combo"]
        assert combos
        assert all("combo_templates" in j and "template_name" not in j for j in combos)
        assert combos[0]["combo_templates"] == ",".join(mod.COMBO_TEMPLATES[0])

    def test_every_job_has_all_three_composite_strategies(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        strategy_names = {j["_strategy_name"] for j in jobs}
        assert strategy_names == set(mod.COMPOSITE_STRATEGIES.keys())

    def test_max_defensive_has_all_filters_stacked(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        max_def = [j for j in jobs if j["_strategy_name"] == "max_defensive"][0]
        assert max_def["min_adtv_cr"] == 0.1
        assert max_def["quality_gate_min_f_score"] == 4
        assert max_def["circuit_band_pct"] == 0.19
        assert max_def["downtrend_filter_pct"] == 0.05
        assert max_def["disable_buys_in_regime"] == "bear"

    def test_balanced_has_no_downtrend_or_regime_filter(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        balanced = [j for j in jobs if j["_strategy_name"] == "balanced"][0]
        assert "downtrend_filter_pct" not in balanced
        assert "disable_buys_in_regime" not in balanced

    def test_jobs_defer_db_writes_for_parallel_safety(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        assert all(j["defer_db_writes"] is True for j in jobs)

    def test_jobs_carry_precomputed_matches_dir(self):
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)
        assert all(j["precomputed_matches_dir"] == str(base_mod.SCREENER_CACHE_DIR) for j in jobs)


class TestStripBookkeeping:
    def test_removes_underscore_keys(self):
        job = {"template_name": "A1", "_strategy_name": "balanced", "_variant_kind": "single"}
        stripped = mod._strip_bookkeeping(job)
        assert stripped == {"template_name": "A1"}


class TestSignalFailureBreakdown:
    def test_no_run_id_returns_empty(self):
        result = mod._signal_failure_breakdown_from_trade_book(None)
        assert result["n_losing_trades"] == 0

    def test_missing_file_returns_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(mod, "REPORTS_DIR", tmp_path)
        result = mod._signal_failure_breakdown_from_trade_book("nonexistent_run")
        assert result["n_losing_trades"] == 0

    def test_parses_trade_book_csv_and_separates_winners_losers(self, tmp_path, monkeypatch):
        # _signal_failure_breakdown_from_trade_book uses this module's own
        # REPORTS_DIR (imported by value from run_technical_experimentation
        # at module load) — patch it here, not on the base module.
        monkeypatch.setattr(mod, "REPORTS_DIR", tmp_path)
        path = tmp_path / "trade_book_run1.csv"
        rows = [
            {"ticker": "A", "buy_date": "2024-01-01", "sell_date": "2024-02-01", "pnl_pct": "0.05",
             "entry_indicator_values": json.dumps({"matched": True, "matched_conditions": 9, "total_conditions": 10}),
             "entry_signal_score": "80"},
            {"ticker": "B", "buy_date": "2024-01-01", "sell_date": "2024-02-01", "pnl_pct": "-0.03",
             "entry_indicator_values": json.dumps({"matched": True, "matched_conditions": 5, "total_conditions": 10}),
             "entry_signal_score": "40"},
        ]
        fieldnames = ["ticker", "buy_date", "sell_date", "pnl_pct", "entry_indicator_values", "entry_signal_score"]
        import csv
        with open(path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        result = mod._signal_failure_breakdown_from_trade_book("run1")
        assert result["n_losing_trades"] == 1
        assert result["n_winning_trades"] == 1
        assert result["losing_trades"][0]["ticker"] == "B"
        assert result["mean_matched_conditions_ratio_losers"] == 0.5
        assert result["mean_matched_conditions_ratio_winners"] == 0.9


class TestAggregateReport:
    def test_aggregates_with_signal_failures(self, tmp_path, monkeypatch):
        monkeypatch.setattr(base_mod, "REPORTS_DIR", tmp_path)
        monkeypatch.setattr(mod, "REPORTS_DIR", tmp_path)
        jobs = mod.build_jobs(date(2016, 1, 1), date(2026, 1, 1), quick=True)[:1]
        suffix = "testsuffix"
        report0 = {"run": {"run_id": "run1"}, "metrics": {"cagr": 0.2, "total_trades": 10}}
        (tmp_path / "orchestrator_testsuffix_job0.json").write_text(json.dumps(report0))

        result = mod.aggregate_report(jobs, suffix)
        assert result["n_jobs_reported"] == 1
        assert result["variants"][0]["cagr"] == 0.2
        assert "signal_failures" in result["variants"][0]
        assert result["variants"][0]["signal_failures"]["n_losing_trades"] == 0  # no trade_book file present

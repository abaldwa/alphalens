"""
tests/unit/test_backfill_fundamentals_nse_xbrl.py

A61 (2026-07-10): unit tests for scripts/backfill_fundamentals_nse_xbrl.py's
_derive_ratios_from_raw — computes debt_to_equity/ebitda_margin/
asset_turnover/roe from raw NSE XBRL fields (+ another source's
revenue/pat/ebitda for the same ticker/fiscal_year/quarter, where NSE XBRL
itself never parses those three). Never overwrites an already-populated
ratio; never fabricates a value when a required raw input is missing.
"""

from scripts.backfill_fundamentals_nse_xbrl import _derive_ratios_from_raw


class TestDeriveRatiosFromRaw:
    def test_debt_to_equity_is_self_contained_no_other_source_needed(self):
        record = {
            "equity_share_capital": 100.0, "other_equity": 400.0,
            "borrowings_current": 50.0, "borrowings_noncurrent": 150.0,
            "debt_to_equity": None, "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        _derive_ratios_from_raw(record, other_source_row=None)
        assert record["debt_to_equity"] == 200.0 / 500.0
        assert record["ebitda_margin"] is None
        assert record["asset_turnover"] is None
        assert record["roe"] is None

    def test_ebitda_margin_asset_turnover_roe_need_other_source_row(self):
        record = {
            "equity_share_capital": 100.0, "other_equity": 400.0,
            "borrowings_current": None, "borrowings_noncurrent": None,
            "total_assets": 1000.0,
            "debt_to_equity": None, "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        other = {"revenue": 800.0, "pat": 100.0, "ebitda": 200.0}
        _derive_ratios_from_raw(record, other_source_row=other)
        assert record["debt_to_equity"] is None  # no borrowings data
        assert record["ebitda_margin"] == 200.0 / 800.0
        assert record["asset_turnover"] == 800.0 / 1000.0
        assert record["roe"] == 100.0 / 500.0

    def test_never_overwrites_an_already_populated_ratio(self):
        record = {
            "equity_share_capital": 100.0, "other_equity": 400.0,
            "borrowings_current": 50.0, "borrowings_noncurrent": 150.0,
            "total_assets": 1000.0,
            "debt_to_equity": 0.11,  # NSE XBRL somehow already reported this directly
            "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        other = {"revenue": 800.0, "pat": 100.0, "ebitda": 200.0}
        _derive_ratios_from_raw(record, other_source_row=other)
        assert record["debt_to_equity"] == 0.11  # untouched

    def test_no_other_source_row_leaves_cross_source_ratios_null(self):
        record = {
            "equity_share_capital": 100.0, "other_equity": 400.0,
            "borrowings_current": None, "borrowings_noncurrent": None,
            "total_assets": 1000.0,
            "debt_to_equity": None, "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        _derive_ratios_from_raw(record, other_source_row=None)
        assert record["ebitda_margin"] is None
        assert record["asset_turnover"] is None
        assert record["roe"] is None

    def test_missing_total_equity_leaves_debt_to_equity_and_roe_null(self):
        record = {
            "equity_share_capital": None, "other_equity": None,
            "borrowings_current": 50.0, "borrowings_noncurrent": 150.0,
            "total_assets": 1000.0,
            "debt_to_equity": None, "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        other = {"revenue": 800.0, "pat": 100.0, "ebitda": 200.0}
        _derive_ratios_from_raw(record, other_source_row=other)
        assert record["debt_to_equity"] is None
        assert record["roe"] is None
        assert record["asset_turnover"] == 800.0 / 1000.0  # unaffected, doesn't need equity

    def test_partial_other_source_row_only_fills_what_it_can(self):
        record = {
            "equity_share_capital": 100.0, "other_equity": 400.0,
            "borrowings_current": None, "borrowings_noncurrent": None,
            "total_assets": 1000.0,
            "debt_to_equity": None, "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        other = {"revenue": 800.0, "pat": None, "ebitda": None}  # only revenue known
        _derive_ratios_from_raw(record, other_source_row=other)
        assert record["asset_turnover"] == 800.0 / 1000.0
        assert record["ebitda_margin"] is None  # no ebitda
        assert record["roe"] is None  # no pat

    def test_zero_denominator_never_divides_by_zero(self):
        record = {
            "equity_share_capital": 0.0, "other_equity": 0.0,
            "borrowings_current": 50.0, "borrowings_noncurrent": 150.0,
            "total_assets": 0.0,
            "debt_to_equity": None, "ebitda_margin": None, "asset_turnover": None, "roe": None,
        }
        other = {"revenue": 0.0, "pat": 100.0, "ebitda": 200.0}
        _derive_ratios_from_raw(record, other_source_row=other)
        assert record["debt_to_equity"] is None
        assert record["asset_turnover"] is None
        assert record["ebitda_margin"] is None
        assert record["roe"] is None

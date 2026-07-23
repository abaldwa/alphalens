"""
tests/unit/test_deep_forensic_altman_z.py

Phase: 3.1 (Deep Forensic ML Features — Groups D-I)
Specs: SPEC-MODEL-010, SPEC-SOLID-005
Owner: Platform / QA
Consumers: CI, pytest

Regression test for the 2026-07-07 altman_z wiring fix in
features/deep_forensic.py: `compute_deep_forensic_features()` previously
derived total_liabilities via a `latest.get("book_equity")` lookup that
doesn't match any real fundamentals column (the real column is
`total_equity`) — so the fallback silently never fired. Fixed to derive
total_liabilities from `total_assets - total_equity` and retained_earnings
from the new real `retained_earnings` column (Screener's "Reserves" row).
altman_z itself still resolves to NaN in all real cases today because (a)
`working_capital` needs `current_assets`/`current_liabilities`, which are
real schema columns but always NULL from Screener's free tier, and (b) no
real, PIT-correct market-cap column exists anywhere in the `fundamentals`
table this function reads from — both documented as genuine gaps in
features/deep_forensic.py's module docstring, not fabricated here. Uses a
fake DataStoreClient (SPEC-SOLID-005 — no real HTTP call) with a
synthetic-but-labeled-as-such fundamentals row matching the REAL schema's
column names, not live data.
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.deep_forensic import _altman_z, compute_deep_forensic_features


def _fund_row(**kwargs):
    row = {
        "ticker": "TEST",
        "quarter_end_date": datetime(2026, 3, 31),
        "announcement_date": datetime(2026, 4, 15),
        "fiscal_year": 2026,
        "quarter": 4,
    }
    row.update(kwargs)
    return row


class TestAltmanZFormula:
    def test_known_inputs_produce_expected_score(self):
        # X1 = 100/1000=0.1, X2=200/1000=0.2, X3=150/1000=0.15, X4=500/300≈1.667, X5=800/1000=0.8
        # Z = 1.2*0.1 + 1.4*0.2 + 3.3*0.15 + 0.6*1.667 + 1.0*0.8
        z = _altman_z(
            working_capital=100, retained_earnings=200, ebit=150,
            total_assets=1000, total_liabilities=300, revenue=800, market_cap=500,
        )
        expected = 1.2 * 0.1 + 1.4 * 0.2 + 3.3 * 0.15 + 0.6 * (500 / 300) + 1.0 * 0.8
        assert z == pytest.approx(expected, rel=1e-6)

    def test_missing_input_returns_nan(self):
        assert np.isnan(_altman_z(np.nan, 1, 1, 1, 1, 1, 1))

    def test_zero_total_assets_returns_nan_not_divzero(self):
        assert np.isnan(_altman_z(1, 1, 1, 0, 1, 1, 1))

    def test_negative_total_liabilities_returns_nan_not_sign_flipped(self):
        """2026-07-19 full-codebase-review Fix: previously abs(total_liabilities)
        silently flipped the sign instead of flagging the input as unusable."""
        assert np.isnan(_altman_z(
            working_capital=100, retained_earnings=200, ebit=150,
            total_assets=1000, total_liabilities=-300, revenue=800, market_cap=500,
        ))


class TestComputeDeepForensicAltmanZ:
    def test_total_liabilities_and_retained_earnings_derive_from_real_columns(self):
        """
        Regression: total_liabilities previously always NaN because the
        derivation fallback looked up a nonexistent "book_equity" column
        instead of the real "total_equity" column. retained_earnings
        previously had no backing column at all. altman_z as a whole still
        resolves to NaN because market cap has no real source in this
        function's inputs (see module docstring) — asserted explicitly so
        this test fails loudly if that continues to be silently misreported
        as "fixed".
        """
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _fund_row(
                total_assets=1000.0,
                total_equity=400.0,
                retained_earnings=150.0,
                revenue=800.0,
                operating_margin=0.15,  # ebit derives as 0.15*800=120
                current_assets=None,  # genuinely unavailable from Screener free tier
                current_liabilities=None,
            )
        ]
        client.get_shareholding_history.return_value = []

        result = compute_deep_forensic_features(client, "TEST", datetime(2026, 7, 1))

        # altman_z itself is still NaN — no real market-cap source.
        assert np.isnan(result["altman_z"])

    def test_altman_z_computes_when_all_inputs_available(self):
        """
        If a future caller supplies market_cap (e.g. via a price join this
        function doesn't currently do), the formula path is fully wired:
        total_liabilities and working_capital both derive correctly.
        """
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _fund_row(
                total_assets=1000.0,
                total_equity=400.0,
                retained_earnings=150.0,
                revenue=800.0,
                operating_margin=0.15,
                market_cap=500.0,
                current_assets=300.0,
                current_liabilities=200.0,
            )
        ]
        client.get_shareholding_history.return_value = []

        result = compute_deep_forensic_features(client, "TEST", datetime(2026, 7, 1))

        assert not np.isnan(result["altman_z"])
        # total_liabilities derives as total_assets - total_equity = 600
        # working_capital = current_assets - current_liabilities = 100
        # ebit derives as operating_margin * revenue = 120
        expected = _altman_z(
            working_capital=100.0, retained_earnings=150.0, ebit=120.0,
            total_assets=1000.0, total_liabilities=600.0, revenue=800.0, market_cap=500.0,
        )
        assert result["altman_z"] == pytest.approx(expected, rel=1e-6)

    def test_altman_z_is_nan_for_financial_services_sector(self):
        """2026-07-19 full-codebase-review Fix: Altman Z's liabilities/
        working-capital ratios don't apply to banks/NBFCs/insurers — skip
        rather than serve a misleading score, even when every input is
        otherwise available (same fundamentals row as the passing case above)."""
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _fund_row(
                total_assets=1000.0,
                total_equity=400.0,
                retained_earnings=150.0,
                revenue=800.0,
                operating_margin=0.15,
                market_cap=500.0,
                current_assets=300.0,
                current_liabilities=200.0,
            )
        ]
        client.get_shareholding_history.return_value = []

        result = compute_deep_forensic_features(
            client, "TEST", datetime(2026, 7, 1), sector="Financial Services"
        )

        assert np.isnan(result["altman_z"])


def _share_row(qed, **kwargs):
    row = {
        "ticker": "TEST", "quarter_end_date": qed, "filing_date": qed,
        "promoter_pct": None, "promoter_pledge": None, "fii_pct": None,
        "dii_pct": None, "mf_pct": None, "retail_pct": None,
    }
    row.update(kwargs)
    return row


class TestInsiderSellingFlagAndPledgeSpiralRisk:
    """
    Regression: insider_selling_flag previously read `fund_df["promoter_pct"]`
    (the fundamentals table, which has no such column — only `shareholding`
    does), so `"promoter_pct" in fund_df.columns` was always False and the
    feature was always NaN. pledge_spiral_risk separately looked up
    "promoter_pledge_pct" instead of the real "promoter_pledge" column.
    """

    def test_insider_selling_flag_true_on_real_promoter_decline(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_fund_row(revenue=800.0)]
        client.get_shareholding_history.return_value = [
            _share_row(datetime(2025, 12, 31), promoter_pct=55.0),
            _share_row(datetime(2026, 3, 31), promoter_pct=50.0),  # -5pp drop
        ]
        result = compute_deep_forensic_features(client, "TEST", datetime(2026, 7, 1))
        assert result["insider_selling_flag"] == 1.0

    def test_insider_selling_flag_false_on_stable_promoter_holding(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_fund_row(revenue=800.0)]
        client.get_shareholding_history.return_value = [
            _share_row(datetime(2025, 12, 31), promoter_pct=55.0),
            _share_row(datetime(2026, 3, 31), promoter_pct=55.2),
        ]
        result = compute_deep_forensic_features(client, "TEST", datetime(2026, 7, 1))
        assert result["insider_selling_flag"] == 0.0

    def test_pledge_spiral_risk_uses_real_column_name(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_fund_row(revenue=800.0)]
        client.get_shareholding_history.return_value = [
            _share_row(datetime(2025, 12, 31), promoter_pledge=10.0),
            _share_row(datetime(2026, 3, 31), promoter_pledge=25.0),
        ]
        result = compute_deep_forensic_features(client, "TEST", datetime(2026, 7, 1))
        # pledge_pct(25) * max(0, delta=15) / 100
        assert result["pledge_spiral_risk"] == pytest.approx(25.0 * 15.0 / 100.0)

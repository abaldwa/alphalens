"""
tests/unit/test_forensic_classical_altman_wiring.py

Phase: 2.5 (Forensic Accounting System M-09/M-10)
Specs: SPEC-MODEL-009, SPEC-PIPE-003 (CRITICAL), SPEC-SOLID-005
Owner: Platform / QA

Regression tests for the 2026-07-13 FO1/FO9 wiring fixes in
features/forensic_classical.py's compute_forensic_classical_scores():

1. `ebit` is now read from the real `ebit` column (added to both
   datastore/api/routers/fundamentals.py's `_COLUMNS` SELECT list and
   datastore/api/schemas.py's FundamentalsWrite model, which previously
   silently dropped it from every GET response) instead of always falling
   back to the ebitda proxy.
2. Altman Z's retained-earnings term (`re`) now reads the real
   `retained_earnings` column instead of proxying it via book equity
   (shares_outstanding x book_value_per_share).
3. Altman Z's market-cap term (`mktcap`) is now built from a real,
   PIT-safe close price (via features.fundamental._latest_close_on_or_before,
   the same helper features/fundamental.py and features/deep_forensic.py
   already use) x shares_outstanding, instead of the book-equity proxy.

Uses a fake DataStoreClient (SPEC-SOLID-005 — no real HTTP call) with a
synthetic-but-labeled-as-such fundamentals row matching the REAL schema's
column names, not live data.
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.forensic_classical import compute_forensic_classical_scores


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


def _base_row(**overrides):
    row = dict(
        current_assets=300.0,
        current_liabilities=200.0,
        total_debt=250.0,
        revenue=800.0,
        shares_outstanding=1_000_000,
        book_value_per_share=100.0,  # book_equity_cr proxy = 1e6*100/1e7 = 10 Cr
        retained_earnings=150.0,
        ebit=120.0,
        ebitda=180.0,
    )
    row.update(overrides)
    return _fund_row(**row)


class TestRetainedEarningsWiring:
    def test_uses_real_retained_earnings_not_book_equity_proxy(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_base_row()]
        client.get_ohlcv.return_value = []  # no close price -> mktcap falls back to proxy

        result = compute_forensic_classical_scores(client, "TEST", datetime(2026, 7, 1))

        # book_equity_cr proxy would be 1_000_000*100/1e7 = 10.0; the real
        # retained_earnings column (150.0) must be used instead. We can't
        # read `re` directly off the public API, so assert indirectly via
        # a z_score computed manually with the real value and compare.
        from systems.ml_signal_engine.models.forensic.classical_scores import altman_z_score

        wc = 300.0 - 200.0
        ta = (1_000_000 * 100.0 / 1e7) + 250.0  # derive_total_assets proxy: book_equity + total_debt
        expected_with_real_re = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 120.0, "ta": ta, "mktcap": 10.0, "tl": 250.0, "sales": 800.0}
        )["z_score"]
        expected_with_proxy_re = altman_z_score(
            {"wc": wc, "re": 10.0, "ebit": 120.0, "ta": ta, "mktcap": 10.0, "tl": 250.0, "sales": 800.0}
        )["z_score"]

        assert result["z_score"] == pytest.approx(expected_with_real_re, rel=1e-6)
        assert result["z_score"] != pytest.approx(expected_with_proxy_re, rel=1e-6)

    def test_falls_back_to_book_equity_proxy_when_retained_earnings_missing(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_base_row(retained_earnings=None)]
        client.get_ohlcv.return_value = []

        result = compute_forensic_classical_scores(client, "TEST", datetime(2026, 7, 1))
        assert not np.isnan(result["z_score"])  # proxy keeps the term computable, not NaN


class TestEbitWiring:
    def test_uses_real_ebit_column_not_ebitda_proxy(self):
        client = MagicMock()
        # ebit (120) deliberately differs from ebitda (180) so a wrong
        # fallback to the proxy would change the resulting z_score.
        client.get_fundamentals_history.return_value = [_base_row(ebit=120.0, ebitda=180.0)]
        client.get_ohlcv.return_value = []

        result = compute_forensic_classical_scores(client, "TEST", datetime(2026, 7, 1))

        from systems.ml_signal_engine.models.forensic.classical_scores import altman_z_score

        wc = 300.0 - 200.0
        ta = (1_000_000 * 100.0 / 1e7) + 250.0
        expected_with_real_ebit = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 120.0, "ta": ta, "mktcap": 10.0, "tl": 250.0, "sales": 800.0}
        )["z_score"]
        expected_with_ebitda_proxy = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 180.0, "ta": ta, "mktcap": 10.0, "tl": 250.0, "sales": 800.0}
        )["z_score"]

        assert result["z_score"] == pytest.approx(expected_with_real_ebit, rel=1e-6)
        assert result["z_score"] != pytest.approx(expected_with_ebitda_proxy, rel=1e-6)

    def test_falls_back_to_ebitda_proxy_when_ebit_missing(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_base_row(ebit=None, ebitda=180.0)]
        client.get_ohlcv.return_value = []

        result = compute_forensic_classical_scores(client, "TEST", datetime(2026, 7, 1))
        assert not np.isnan(result["z_score"])


class TestMarketCapPITWiring:
    def test_uses_real_close_price_times_shares_when_available(self):
        as_of = datetime(2026, 7, 1)
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_base_row()]
        # close of 500 on 2026-06-30, on-or-before as_of.
        client.get_ohlcv.return_value = [{"date": "2026-06-30", "close": 500.0}]

        result = compute_forensic_classical_scores(client, "TEST", as_of)

        from systems.ml_signal_engine.models.forensic.classical_scores import altman_z_score

        wc = 300.0 - 200.0
        ta = (1_000_000 * 100.0 / 1e7) + 250.0
        real_mktcap = (1_000_000 * 500.0) / 1e7  # 50.0 Cr
        expected_with_real_mktcap = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 120.0, "ta": ta, "mktcap": real_mktcap, "tl": 250.0, "sales": 800.0}
        )["z_score"]
        expected_with_proxy_mktcap = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 120.0, "ta": ta, "mktcap": 10.0, "tl": 250.0, "sales": 800.0}
        )["z_score"]

        assert result["z_score"] == pytest.approx(expected_with_real_mktcap, rel=1e-6)
        assert result["z_score"] != pytest.approx(expected_with_proxy_mktcap, rel=1e-6)

    def test_market_cap_join_uses_the_same_ohlcv_pit_boundary_as_of(self):
        """
        _latest_close_on_or_before (features/fundamental.py, reused
        unmodified here — the established PIT-correct pattern) requests
        `to_date=as_of`; this asserts compute_forensic_classical_scores
        forwards the real `as_of` it was given rather than an unbounded or
        future date, so no not-yet-knowable close price can leak in.
        """
        as_of = datetime(2026, 7, 1)
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_base_row()]
        client.get_ohlcv.return_value = [{"date": "2026-06-30", "close": 500.0}]

        compute_forensic_classical_scores(client, "TEST", as_of)

        assert client.get_ohlcv.called
        _, call_kwargs = client.get_ohlcv.call_args
        assert call_kwargs["to_date"] == as_of

    def test_shares_outstanding_does_not_leak_a_not_yet_filed_future_quarter(self):
        """
        PIT correctness for the market-cap join's shares_outstanding term:
        get_fundamentals_history() is the server-side PIT filter (SPEC-
        PIPE-003) — announcement_date <= as_of. This simulates a real PIT-
        filtering client (like the real API) and asserts the shares_
        outstanding used for market_cap comes from the PIT-eligible
        (already-announced) quarter, never a later quarter whose
        announcement_date is still in the future relative to `as_of`.
        """
        as_of = datetime(2026, 7, 1)
        all_rows = [
            _base_row(
                quarter_end_date=datetime(2025, 12, 31),
                announcement_date=datetime(2026, 1, 15),  # PIT-eligible as of 2026-07-01
                fiscal_year=2026, quarter=3,
                shares_outstanding=1_000_000,
            ),
            _base_row(
                quarter_end_date=datetime(2026, 6, 30),
                announcement_date=datetime(2026, 7, 10),  # NOT YET FILED as of 2026-07-01
                fiscal_year=2026, quarter=4,
                shares_outstanding=2_000_000,  # would double market_cap if leaked
            ),
        ]

        def _get_fundamentals_history(ticker, req_as_of, lookback_years=4):
            return [r for r in all_rows if r["announcement_date"] <= req_as_of]

        client = MagicMock()
        client.get_fundamentals_history.side_effect = _get_fundamentals_history
        client.get_ohlcv.return_value = [{"date": "2026-06-30", "close": 500.0}]

        result = compute_forensic_classical_scores(client, "TEST", as_of)

        from systems.ml_signal_engine.models.forensic.classical_scores import altman_z_score

        wc = 300.0 - 200.0
        ta = (1_000_000 * 100.0 / 1e7) + 250.0
        pit_correct_mktcap = (1_000_000 * 500.0) / 1e7  # uses the PIT-eligible shares_outstanding
        leaked_mktcap = (2_000_000 * 500.0) / 1e7  # what a PIT bug would produce
        expected = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 120.0, "ta": ta, "mktcap": pit_correct_mktcap, "tl": 250.0, "sales": 800.0}
        )["z_score"]
        leaked = altman_z_score(
            {"wc": wc, "re": 150.0, "ebit": 120.0, "ta": ta, "mktcap": leaked_mktcap, "tl": 250.0, "sales": 800.0}
        )["z_score"]

        assert result["z_score"] == pytest.approx(expected, rel=1e-6)
        assert result["z_score"] != pytest.approx(leaked, rel=1e-6)

    def test_close_price_lookup_failure_falls_back_to_proxy_not_crash(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [_base_row()]
        client.get_ohlcv.side_effect = RuntimeError("API unreachable")

        result = compute_forensic_classical_scores(client, "TEST", datetime(2026, 7, 1))
        assert not np.isnan(result["z_score"])  # degrades to the book-equity proxy, never raises

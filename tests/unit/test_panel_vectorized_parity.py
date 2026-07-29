"""
tests/unit/test_panel_vectorized_parity.py

Phase: N/A (post-Phase-2 optimization, 2026-07-29)
Specs: SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004
Owner: Platform / QA

Row-for-row parity tests between the CURRENT sequential panel functions
(compute_governance_features_panel, compute_corporate_action_features_panel,
compute_mf_holdings_features_panel — the shipped, `panel_workers=1`
production baseline) and the new vectorized alternatives
(compute_governance_features_panel_vectorized,
compute_corporate_action_features_panel_vectorized,
compute_mf_holdings_features_panel_vectorized).

Covers, per the 2026-07-29 model-review's mandatory point 6:
  - a ticker with a single filing/action (no QoQ/prior comparison possible)
  - a ticker with zero history
  - a ticker with sparse/no OHLCV in the pledge-spiral / anticipation-return window
  - a synthetic same-(ticker, quarter_end_date) duplicate-filing fixture
    (2026-07-29: confirmed against the live DuckDB `shareholding` table
    that zero real duplicates exist today — this fixture is a defensive,
    not a regression, test)
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from features.corporate_action_features import (
    compute_corporate_action_features_panel,
    compute_corporate_action_features_panel_vectorized,
)
from features.governance import (
    compute_governance_features_panel,
    compute_governance_features_panel_vectorized,
)
from features.mf_holdings import (
    compute_mf_holdings_features_panel,
    compute_mf_holdings_features_panel_vectorized,
)


def _filing(qed, fd, **kwargs):
    row = {
        "quarter_end_date": qed, "filing_date": fd,
        "promoter_pct": None, "promoter_pledge": None, "fii_pct": None,
        "dii_pct": None, "mf_pct": None, "retail_pct": None,
    }
    row.update(kwargs)
    return row


class TestGovernancePanelParity:
    def _client(self, data):
        client = MagicMock()
        client.get_shareholding_history.side_effect = lambda ticker, as_of, lookback_years=2: data.get(ticker, [])
        client.get_ohlcv.return_value = []
        return client

    def test_parity_across_realistic_scenarios(self):
        data = {
            # Two filings — normal QoQ comparison.
            "MULTI": [
                _filing("2025-03-31", "2025-04-21", promoter_pct=50.0, promoter_pledge=25.0, fii_pct=20.0, dii_pct=15.0, mf_pct=5.0),
                _filing("2025-06-30", "2025-07-21", promoter_pct=51.5, promoter_pledge=30.0, fii_pct=19.0, dii_pct=16.0, mf_pct=5.5),
            ],
            # Single filing — qoq_prior is None.
            "SINGLE": [
                _filing("2025-03-31", "2025-04-21", promoter_pct=60.0),
            ],
            # Zero shareholding history.
            "NOHIST": [],
            # High pledge, but no OHLCV window rows at all (sparse/delisted).
            "SPARSE_OHLCV": [
                _filing("2025-03-31", "2025-04-21", promoter_pct=40.0, promoter_pledge=35.0),
            ],
            # High pledge with a real falling-price window.
            "SPIRAL": [
                _filing("2025-03-31", "2025-04-21", promoter_pct=40.0, promoter_pledge=40.0),
            ],
        }
        client = self._client(data)

        ohlcv_panel = pd.DataFrame([
            {"ticker": "SPIRAL", "date": pd.Timestamp("2025-06-01"), "close": 100.0},
            {"ticker": "SPIRAL", "date": pd.Timestamp("2025-07-25"), "close": 70.0},
        ])

        tickers = list(data.keys())
        as_of = datetime(2025, 8, 1)

        sequential = compute_governance_features_panel(client, tickers, as_of, ohlcv_panel=ohlcv_panel)
        vectorized = compute_governance_features_panel_vectorized(client, tickers, as_of, ohlcv_panel=ohlcv_panel)

        seq = sequential.sort_values("ticker").reset_index(drop=True)
        vec = vectorized.sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(seq, vec, check_dtype=False)

        # Explicit assertions on the interesting rows.
        spiral_row = vec[vec["ticker"] == "SPIRAL"].iloc[0]
        assert spiral_row["promoter_pledge_spiral_flag"] == 1
        sparse_row = vec[vec["ticker"] == "SPARSE_OHLCV"].iloc[0]
        assert sparse_row["promoter_pledge_spiral_flag"] == 0
        nohist_row = vec[vec["ticker"] == "NOHIST"].iloc[0]
        assert np.isnan(nohist_row["promoter_pct"])
        single_row = vec[vec["ticker"] == "SINGLE"].iloc[0]
        assert np.isnan(single_row["promoter_change_qoq"])

    def test_synthetic_duplicate_filing_documents_known_divergence(self):
        """
        Defensive fixture: two rows sharing the same (ticker,
        quarter_end_date) but different filing_date/values — reproduces
        the *shape* a real restated filing would take. Confirmed against
        the live DuckDB `shareholding` table (2026-07-29): zero such
        duplicates exist in production today, so this is a purely
        synthetic edge case with no current production impact.

        KNOWN, DOCUMENTED DIVERGENCE (reported, not silently papered
        over): the sequential function has NO dedup logic at all —
        `history.sort_values("quarter_end_date").iloc[-1]`/`iloc[-2]`
        blindly treats the LAST TWO ROWS as "latest"/"prior" regardless
        of whether they share a quarter_end_date, so with only two rows
        that happen to share the same quarter, it computes a spurious
        QoQ delta between two same-quarter filings (promoter_change_qoq
        == 2.0 in this fixture, comparing a quarter's two restated
        filings to each other as if they were sequential quarters).

        The vectorized function implements the review's mandatory point
        2 (explicit dedup on (ticker, quarter_end_date), keep="last")
        BEFORE computing shift(1) — this is the demanded fix, not a bug:
        after dedup there is only one row for this ticker's single real
        quarter, so `promoter_change_qoq` is correctly NaN (no second
        quarter exists to compare against), matching
        `test_single_quarter_has_no_qoq_change`'s existing precedent.

        Net effect: for this synthetic duplicate-only fixture, the two
        panel functions intentionally do NOT produce bit-for-bit-identical
        output — the vectorized version is the corrected one. Since no
        real duplicates exist today, this has zero live impact, but it
        is called out explicitly rather than forced into false parity.
        """
        data = {
            "DUPCO": [
                _filing("2025-06-30", "2025-07-10", promoter_pct=50.0, fii_pct=10.0),
                # Same quarter_end_date, later filing_date and different values
                # (e.g. a restated figure) — inserted AFTER the first row.
                _filing("2025-06-30", "2025-07-25", promoter_pct=52.0, fii_pct=11.0),
            ],
        }
        client = self._client(data)
        tickers = ["DUPCO"]
        as_of = datetime(2025, 8, 1)

        sequential = compute_governance_features_panel(client, tickers, as_of)
        vectorized = compute_governance_features_panel_vectorized(client, tickers, as_of)

        # Sequential (current production, no dedup): spurious non-NaN QoQ
        # delta between the two same-quarter rows.
        assert sequential.iloc[0]["promoter_change_qoq"] == 2.0
        # Vectorized (with the mandatory dedup fix applied): correctly NaN
        # — there is only one real quarter for this ticker.
        assert np.isnan(vectorized.iloc[0]["promoter_change_qoq"])
        # Both agree on the "latest" snapshot value itself (post-dedup,
        # keep="last" picks the same final promoter_pct sequential's
        # iloc[-1] would see, since both take the last-sorted row).
        assert sequential.iloc[0]["promoter_pct"] == vectorized.iloc[0]["promoter_pct"] == 52.0


class TestCorporateActionPanelParity:
    def test_parity_across_realistic_scenarios(self):
        client = MagicMock()
        actions = {
            "MULTI": [
                {"ex_date": "2025-06-01", "action_type": "SPLIT", "ratio": 2.0, "announcement_date": None, "record_date": "2025-05-25"},
            ],
            "SINGLE": [
                {"ex_date": "2025-01-01", "action_type": "BONUS", "ratio": 1.0, "announcement_date": None, "record_date": None},
            ],
            "NOHIST": [],
            "BUYBACK_NO_OHLCV": [
                {"ex_date": "2025-07-15", "action_type": "BUYBACK", "ratio": 150.0, "announcement_date": None, "record_date": None},
            ],
        }
        client.get_corporate_actions.side_effect = lambda ticker: actions.get(ticker, [])
        client.get_fundamentals_history.side_effect = lambda ticker, as_of, lookback_years=1: []
        client.get_ohlcv.return_value = []

        ohlcv_panel = pd.DataFrame([
            {"ticker": "MULTI", "date": pd.Timestamp("2025-05-29"), "close": 90.0},
            {"ticker": "MULTI", "date": pd.Timestamp("2025-05-30"), "close": 95.0},
            # BUYBACK_NO_OHLCV is deliberately absent from ohlcv_panel entirely.
        ])

        tickers = list(actions.keys())
        as_of = datetime(2025, 8, 1)
        listing_dates = {"SINGLE": datetime(2024, 1, 1)}

        sequential = compute_corporate_action_features_panel(
            client, tickers, as_of, listing_dates=listing_dates, ohlcv_panel=ohlcv_panel
        )
        vectorized = compute_corporate_action_features_panel_vectorized(
            client, tickers, as_of, listing_dates=listing_dates, ohlcv_panel=ohlcv_panel
        )

        seq = sequential.sort_values("ticker").reset_index(drop=True)
        vec = vectorized.sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(seq, vec, check_dtype=False)


class TestMFHoldingsPanelParity:
    def test_parity_across_realistic_scenarios(self, tmp_path):
        month1 = pd.DataFrame([
            {"scheme_name": "ABC Smallcap Fund", "isin": "X1", "ticker": "MULTI", "quantity": 100, "value_inr": 1000.0, "month": "2025-05", "availability_date": "2025-05-05"},
            {"scheme_name": "XYZ Largecap Fund", "isin": "X2", "ticker": "MULTI", "quantity": 50, "value_inr": 500.0, "month": "2025-05", "availability_date": "2025-05-05"},
        ])
        month2 = pd.DataFrame([
            {"scheme_name": "ABC Smallcap Fund", "isin": "X1", "ticker": "MULTI", "quantity": 120, "value_inr": 1200.0, "month": "2025-06", "availability_date": "2025-06-05"},
            {"scheme_name": "NEW Fund", "isin": "X3", "ticker": "MULTI", "quantity": 10, "value_inr": 100.0, "month": "2025-06", "availability_date": "2025-06-05"},
        ])
        month1.to_parquet(tmp_path / "2025-05.parquet")
        month2.to_parquet(tmp_path / "2025-06.parquet")

        tickers = ["MULTI", "NOHIST"]
        as_of = datetime(2025, 7, 1)
        tier_map = {"MULTI": 1, "NOHIST": 2}

        sequential = compute_mf_holdings_features_panel(tickers, as_of, tier_map=tier_map, holdings_dir=tmp_path)
        vectorized = compute_mf_holdings_features_panel_vectorized(tickers, as_of, tier_map=tier_map, holdings_dir=tmp_path)

        seq = sequential.sort_values("ticker").reset_index(drop=True)
        vec = vectorized.sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(seq, vec, check_dtype=False)

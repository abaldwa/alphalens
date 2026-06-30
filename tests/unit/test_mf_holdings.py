"""
tests/unit/test_mf_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004
Owner: Platform / QA
Consumers: CI, pytest

SPEC-PIPE-003: MF holdings features for date=2024-06-01 must use only May
2024 data — June 2024 holdings aren't disclosed until ~2024-07-05. Tests
write real Parquet files (matching ingestion/scrapers/amfi_holdings.py's
save_monthly_parquet output shape) to a tmp_path and exercise
load_mf_holdings_history()'s real file I/O, not mocks.
"""

from datetime import datetime

import pandas as pd
import pytest

from features.mf_holdings import (
    MF_HOLDINGS_FEATURES,
    compute_mf_holdings_features,
    compute_mf_holdings_features_panel,
    find_dii_entry_exit_signals,
    load_mf_holdings_history,
)


def _write_month(holdings_dir, year, month, rows, availability_date):
    holdings_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=["scheme_name", "isin", "ticker", "quantity", "value_inr"])
    df["month"] = f"{year:04d}-{month:02d}"
    df["availability_date"] = availability_date
    df.to_parquet(holdings_dir / f"{year:04d}-{month:02d}.parquet", index=False)


MAY_ROWS = [
    ("SBI Bluechip Fund", "INF200K01158", "RELIANCE", 1000, 1310000.0),
    ("HDFC Top 100 Fund", "INF179K01158", "RELIANCE", 2000, 2620000.0),
    ("ICICI Pru Smallcap Fund", "INF109K01258", "RELIANCE", 500, 655000.0),
]
JUNE_ROWS = MAY_ROWS + [
    ("Axis Bluechip Fund", "INF846K01131", "RELIANCE", 800, 1048000.0),
    ("Kotak Smallcap Fund", "INF174K01158", "RELIANCE", 600, 786000.0),
    ("Mirae Asset Largecap Fund", "INF769K01010", "RELIANCE", 400, 524000.0),
]


@pytest.fixture
def holdings_dir(tmp_path):
    d = tmp_path / "mf_holdings"
    # May 2024 -> available 2024-06-05; June 2024 -> available 2024-07-05
    _write_month(d, 2024, 5, MAY_ROWS, availability_date=pd.Timestamp("2024-06-05"))
    _write_month(d, 2024, 6, JUNE_ROWS, availability_date=pd.Timestamp("2024-07-05"))
    return d


class TestPITAlignment:
    """SPEC-PIPE-003 (CRITICAL): mf features for date=2024-06-01 use only May 2024 data, not June."""

    def test_as_of_june_1_sees_no_data_yet_not_even_may(self, holdings_dir):
        """
        SPEC-PIPE-003: "available from ~5th of following month" means May
        2024's disclosure becomes PIT-visible on 2024-06-05 — strictly
        BEFORE 2024-06-01, May itself is not yet public knowledge either.
        The P2.2 build prompt's literal example ("date=2024-06-01 uses
        only May 2024 data") would only hold under a same-day/immediate
        availability assumption; this codebase's actual rule (config.
        settings.MF_HOLDINGS_AVAILABILITY_DELAY_DAYS = 5) is more
        conservative than that, so 2024-06-01 correctly sees ZERO months
        — never May, and certainly never June. The intent behind the
        prompt's example ("don't use the current month's own
        not-yet-disclosed data") is satisfied either way; see
        test_as_of_june_5_uses_only_may_data below for the exact-day
        boundary this system actually enforces.
        """
        as_of = datetime(2024, 6, 1)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)

        assert history.empty

    def test_as_of_june_5_uses_only_may_data(self, holdings_dir):
        """The exact PIT boundary: May becomes visible on 2024-06-05, June not until 2024-07-05."""
        as_of = datetime(2024, 6, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        feats = compute_mf_holdings_features("RELIANCE", as_of, history)

        assert set(history["month"].unique()) == {"2024-05"}
        assert feats["mf_scheme_count"] == 3  # May's 3 schemes, not June's 6

    def test_as_of_july_5_includes_june_disclosure(self, holdings_dir):
        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)

        assert set(history["month"].unique()) == {"2024-05", "2024-06"}

    def test_as_of_july_4_still_excludes_june(self, holdings_dir):
        """Availability is exactly 2024-07-05 — the day before must still exclude it."""
        as_of = datetime(2024, 7, 4)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)

        assert set(history["month"].unique()) == {"2024-05"}


class TestNewEntryAndExitCounts:
    def test_three_new_schemes_in_june_vs_may(self, holdings_dir):
        """3 new schemes in June vs May -> mf_new_entry_count returns 3."""
        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        feats = compute_mf_holdings_features("RELIANCE", as_of, history)

        assert feats["mf_new_entry_count"] == 3
        assert feats["mf_exit_count"] == 0
        assert feats["mf_scheme_count"] == 6
        assert feats["mf_scheme_count_change_1m"] == 3

    def test_exit_count_when_a_scheme_drops_out(self, tmp_path):
        d = tmp_path / "mf_holdings"
        may_rows = MAY_ROWS  # 3 schemes
        june_rows = MAY_ROWS[:2]  # one scheme (ICICI Pru Smallcap) exits
        _write_month(d, 2024, 5, may_rows, pd.Timestamp("2024-06-05"))
        _write_month(d, 2024, 6, june_rows, pd.Timestamp("2024-07-05"))

        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=d)
        feats = compute_mf_holdings_features("RELIANCE", as_of, history)

        assert feats["mf_exit_count"] == 1
        assert feats["mf_new_entry_count"] == 0


class TestSuperstarInvestorFlag:
    def test_flag_triggers_when_tracked_investor_holds_stock(self, holdings_dir):
        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        superstar_holdings = pd.DataFrame(
            [{"ticker": "RELIANCE", "investor_name": "Dolly Khanna", "as_of_date": "2024-06-15", "holding_pct": 1.2}]
        )

        feats = compute_mf_holdings_features("RELIANCE", as_of, history, superstar_holdings=superstar_holdings)

        assert feats["superstar_investor_flag"] == 1

    def test_flag_is_zero_when_no_tracked_investor_holds_stock(self, holdings_dir):
        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        superstar_holdings = pd.DataFrame(
            [{"ticker": "TCS", "investor_name": "Vijay Kedia", "as_of_date": "2024-06-15", "holding_pct": 0.8}]
        )

        feats = compute_mf_holdings_features("RELIANCE", as_of, history, superstar_holdings=superstar_holdings)

        assert feats["superstar_investor_flag"] == 0

    def test_flag_is_zero_when_no_superstar_data_supplied(self, holdings_dir):
        """No Trendlyne integration yet (module docstring) — degrades to 0, not an exception."""
        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)

        feats = compute_mf_holdings_features("RELIANCE", as_of, history, superstar_holdings=None)

        assert feats["superstar_investor_flag"] == 0
        assert feats["superstar_investor_change"] == 0

    def test_change_reflects_increase_and_decrease(self, holdings_dir):
        as_of = datetime(2024, 7, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        increasing = pd.DataFrame(
            [
                {"ticker": "RELIANCE", "investor_name": "Ashish Kacholia",
                 "as_of_date": "2024-05-15", "holding_pct": 0.5},
                {"ticker": "RELIANCE", "investor_name": "Ashish Kacholia",
                 "as_of_date": "2024-06-15", "holding_pct": 0.9},
            ]
        )
        feats = compute_mf_holdings_features("RELIANCE", as_of, history, superstar_holdings=increasing)
        assert feats["superstar_investor_change"] == 1

        decreasing = increasing.copy()
        decreasing.loc[1, "holding_pct"] = 0.2
        feats2 = compute_mf_holdings_features("RELIANCE", as_of, history, superstar_holdings=decreasing)
        assert feats2["superstar_investor_change"] == -1


class TestNoHistory:
    def test_no_data_at_all_returns_nan_not_exception(self, tmp_path):
        as_of = datetime(2024, 6, 1)
        history = load_mf_holdings_history(as_of, holdings_dir=tmp_path / "nonexistent")
        feats = compute_mf_holdings_features("RELIANCE", as_of, history)

        assert set(feats.keys()) == set(MF_HOLDINGS_FEATURES)
        assert feats["mf_new_entry_count"] == 0
        assert feats["superstar_investor_flag"] == 0


class TestConcentrationAndSmallcap:
    def test_concentration_top5_sums_to_one_with_five_or_fewer_schemes(self, holdings_dir):
        as_of = datetime(2024, 6, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        feats = compute_mf_holdings_features("RELIANCE", as_of, history)

        assert feats["mf_concentration_top5"] == pytest.approx(1.0)

    def test_smallcap_fund_holding_matches_name_pattern(self, holdings_dir):
        as_of = datetime(2024, 6, 5)
        history = load_mf_holdings_history(as_of, holdings_dir=holdings_dir)
        feats = compute_mf_holdings_features("RELIANCE", as_of, history)

        assert feats["mf_smallcap_fund_holding"] == 655000.0  # only ICICI Pru Smallcap Fund


class TestPanelCrowdednessRank:
    def test_crowdedness_rank_is_percentile_within_tier(self, tmp_path):
        d = tmp_path / "mf_holdings"
        rows = [
            ("Fund A", "ISIN_A", "HIGH", 100, 100.0),
            ("Fund B", "ISIN_B", "HIGH", 100, 100.0),
            ("Fund A", "ISIN_A", "MED", 100, 100.0),
        ]
        _write_month(d, 2024, 5, rows, pd.Timestamp("2024-06-05"))

        as_of = datetime(2024, 6, 5)
        panel = compute_mf_holdings_features_panel(
            ["HIGH", "MED", "LOW"], as_of, tier_map={"HIGH": 1, "MED": 1, "LOW": 1}, holdings_dir=d
        )

        high_rank = panel[panel["ticker"] == "HIGH"]["mf_crowdedness_rank"].iloc[0]
        low_rank = panel[panel["ticker"] == "LOW"]["mf_crowdedness_rank"].iloc[0]
        assert high_rank > low_rank  # HIGH has 2 schemes, LOW has 0 -> HIGH ranks higher within the same tier


class TestDIIEntryExitSignals:
    def test_entry_signal_when_only_new_schemes_appear(self, tmp_path):
        d = tmp_path / "mf_holdings"
        may_rows = [("Fund A", "ISIN_A", "ENTRYCO", 100, 100.0)]
        june_rows = may_rows + [("Fund B", "ISIN_B", "ENTRYCO", 50, 50.0)]
        _write_month(d, 2024, 5, may_rows, pd.Timestamp("2024-06-05"))
        _write_month(d, 2024, 6, june_rows, pd.Timestamp("2024-07-05"))

        result = find_dii_entry_exit_signals(["ENTRYCO"], datetime(2024, 7, 5), holdings_dir=d)

        row = result[result["ticker"] == "ENTRYCO"].iloc[0]
        assert row["dii_flow_signal"] == "ENTRY"
        assert row["mf_new_entry_count"] == 1
        assert row["mf_exit_count"] == 0

    def test_exit_signal_when_a_scheme_drops_out(self, tmp_path):
        d = tmp_path / "mf_holdings"
        may_rows = [("Fund A", "ISIN_A", "EXITCO", 100, 100.0), ("Fund B", "ISIN_B", "EXITCO", 50, 50.0)]
        june_rows = [("Fund A", "ISIN_A", "EXITCO", 100, 100.0)]
        _write_month(d, 2024, 5, may_rows, pd.Timestamp("2024-06-05"))
        _write_month(d, 2024, 6, june_rows, pd.Timestamp("2024-07-05"))

        result = find_dii_entry_exit_signals(["EXITCO"], datetime(2024, 7, 5), holdings_dir=d)

        row = result[result["ticker"] == "EXITCO"].iloc[0]
        assert row["dii_flow_signal"] == "EXIT"
        assert row["mf_exit_count"] == 1
        assert row["mf_new_entry_count"] == 0

    def test_neutral_signal_when_no_scheme_level_change(self, tmp_path):
        d = tmp_path / "mf_holdings"
        rows = [("Fund A", "ISIN_A", "STABLECO", 100, 100.0)]
        _write_month(d, 2024, 5, rows, pd.Timestamp("2024-06-05"))
        _write_month(d, 2024, 6, rows, pd.Timestamp("2024-07-05"))

        result = find_dii_entry_exit_signals(["STABLECO"], datetime(2024, 7, 5), holdings_dir=d)

        row = result[result["ticker"] == "STABLECO"].iloc[0]
        assert row["dii_flow_signal"] == "NEUTRAL"

    def test_mixed_signal_when_one_enters_and_one_exits(self, tmp_path):
        d = tmp_path / "mf_holdings"
        may_rows = [("Fund A", "ISIN_A", "MIXEDCO", 100, 100.0)]
        june_rows = [("Fund B", "ISIN_B", "MIXEDCO", 50, 50.0)]
        _write_month(d, 2024, 5, may_rows, pd.Timestamp("2024-06-05"))
        _write_month(d, 2024, 6, june_rows, pd.Timestamp("2024-07-05"))

        result = find_dii_entry_exit_signals(["MIXEDCO"], datetime(2024, 7, 5), holdings_dir=d)

        row = result[result["ticker"] == "MIXEDCO"].iloc[0]
        assert row["dii_flow_signal"] == "MIXED"

    def test_sorted_strongest_entries_first(self, tmp_path):
        d = tmp_path / "mf_holdings"
        may_rows = [("Fund A", "ISIN_A", "BIGENTRY", 1, 1.0), ("Fund A", "ISIN_A", "SMALLENTRY", 1, 1.0)]
        june_rows = [
            ("Fund A", "ISIN_A", "BIGENTRY", 1, 1.0),
            ("Fund B", "ISIN_B", "BIGENTRY", 1, 1.0),
            ("Fund C", "ISIN_C", "BIGENTRY", 1, 1.0),
            ("Fund A", "ISIN_A", "SMALLENTRY", 1, 1.0),
            ("Fund B", "ISIN_B", "SMALLENTRY", 1, 1.0),
        ]
        _write_month(d, 2024, 5, may_rows, pd.Timestamp("2024-06-05"))
        _write_month(d, 2024, 6, june_rows, pd.Timestamp("2024-07-05"))

        result = find_dii_entry_exit_signals(["BIGENTRY", "SMALLENTRY"], datetime(2024, 7, 5), holdings_dir=d)

        assert result.iloc[0]["ticker"] == "BIGENTRY"  # 2 new entries > 1 new entry

"""
tests/unit/test_integrity_checks.py

Phase: A20 (Data Integrity Checker)
Owner: Platform / QA

Tests datastore/integrity/checks.py's four checks against a private
in-memory DuckDB connection with small synthetic fixtures (never the real
alphalens.duckdb). Fyers/Yahoo fetches are mocked — no live network calls.
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from datastore.api.db import get_duckdb_connection
from datastore.integrity.checks import (
    check_corporate_actions,
    check_corporate_actions_coverage,
    check_holiday_leakage,
    check_null_sweep,
    check_spot_check,
)
from datastore.schema.create_normalised import create_schema


@pytest.fixture
def conn():
    create_schema(in_memory=True)
    with get_duckdb_connection(None) as c:
        yield c
        c.execute("DELETE FROM ohlcv_adjusted")
        c.execute("DELETE FROM corporate_actions")
        c.execute("DELETE FROM fundamentals")
        c.execute("DELETE FROM macro_indicators")


class _FakeFyers:
    """Injectable stand-in for FYERSBackfill in tests."""

    def __init__(self, history_by_ticker):
        self._history_by_ticker = history_by_ticker

    def download_history(self, ticker, from_date, to_date):
        hist = self._history_by_ticker.get(ticker, pd.DataFrame(columns=["date", "close"]))
        if hist.empty:
            return hist
        mask = (hist["date"] >= from_date) & (hist["date"] <= to_date)
        return hist.loc[mask].reset_index(drop=True)


def _flat_series(ticker, start, days, close):
    dates = pd.date_range(start, periods=days, freq="D")
    return pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), "close": [close] * days})


class TestCheckCorporateActions:
    def test_detects_missing_split(self, conn):
        # Our data never applies the announced 2:1 split — close stays flat.
        ticker = "TESTCO"
        ex_date = date(2026, 6, 1)
        conn.execute(
            "INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio) VALUES (?, ?, 'SPLIT', 2.0)",
            [ticker, ex_date],
        )
        rows = []
        d = ex_date - timedelta(days=30)
        for i in range(61):
            rows.append((d + timedelta(days=i), ticker, 100.0, 100.0, 100.0, 100.0, 1000))
        for r in rows:
            conn.execute(
                "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                r,
            )

        # Fyers shows the real 2:1 split applied (price halves after ex_date).
        fy_rows = []
        for i in range(61):
            dd = d + timedelta(days=i)
            close = 50.0 if dd >= ex_date else 100.0
            fy_rows.append({"date": dd.isoformat(), "close": close})
        fake_fy = _FakeFyers({ticker: pd.DataFrame(fy_rows)})

        findings = check_corporate_actions(conn, date(2026, 6, 5), lookback_days=7, fyers_client=fake_fy)
        assert any(f.severity == "critical" and f.ticker == ticker for f in findings)

    def test_no_finding_when_split_correctly_applied(self, conn):
        ticker = "GOODCO"
        ex_date = date(2026, 6, 1)
        conn.execute(
            "INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio) VALUES (?, ?, 'SPLIT', 2.0)",
            [ticker, ex_date],
        )
        d = ex_date - timedelta(days=30)
        for i in range(61):
            dd = d + timedelta(days=i)
            close = 50.0 if dd >= ex_date else 100.0
            conn.execute(
                "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (dd, ticker, close, close, close, close, 1000),
            )

        fy_rows = []
        for i in range(61):
            dd = d + timedelta(days=i)
            close = 50.0 if dd >= ex_date else 100.0
            fy_rows.append({"date": dd.isoformat(), "close": close})
        fake_fy = _FakeFyers({ticker: pd.DataFrame(fy_rows)})

        findings = check_corporate_actions(conn, date(2026, 6, 5), lookback_days=7, fyers_client=fake_fy)
        assert findings == []


class TestCheckNullSweep:
    def test_flags_column_not_in_known_sparse_list(self, conn):
        d = date(2026, 6, 1)
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_pct) "
            "VALUES (?, 'A', 1, 1, 1, 1, 1, NULL), (?, 'B', 1, 1, 1, 1, 1, NULL)",
            [d, d],
        )
        findings = check_null_sweep(conn, d)
        assert any(f.evidence["column"] == "delivery_pct" for f in findings)

    def test_does_not_flag_known_sparse_column(self, conn):
        d = date(2026, 6, 1)
        conn.execute(
            "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, announcement_date, inventory_days) "
            "VALUES ('A', 2026, 1, ?, ?, NULL)",
            [d, d],
        )
        findings = check_null_sweep(conn, d)
        assert not any(f.evidence.get("column") == "inventory_days" for f in findings)


class TestCheckHolidayLeakage:
    def test_flags_ohlcv_row_on_known_holiday(self, conn):
        # 2026-01-26 is Republic Day, a known NSE holiday (config/nse_holidays.py).
        holiday = date(2026, 1, 26)
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, 'A', 1, 1, 1, 1, 1)",
            [holiday],
        )
        findings = check_holiday_leakage(conn, date(2026, 2, 1), lookback_days=30)
        assert any(f.evidence["leaked_date"] == str(holiday) for f in findings)

    def test_no_finding_for_normal_trading_day(self, conn):
        trading_day = date(2026, 1, 27)
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, 'A', 1, 1, 1, 1, 1)",
            [trading_day],
        )
        findings = check_holiday_leakage(conn, date(2026, 2, 1), lookback_days=30)
        assert findings == []


class TestCheckSpotCheck:
    def test_flags_only_when_both_sources_disagree_with_us(self, conn):
        ticker = "SPOTCO"
        d = date(2026, 6, 1)
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, ?, 100, 100, 100, 100, 1000)",
            [d, ticker],
        )
        fake_fy = _FakeFyers({ticker: pd.DataFrame([{"date": d.isoformat(), "close": 200.0}])})

        def fake_yahoo(t, dd):
            return 200.0

        findings = check_spot_check(
            conn, date(2026, 6, 5), sample_size=10, seed=1, fyers_client=fake_fy, yahoo_fetch=fake_yahoo
        )
        assert any(f.ticker == ticker for f in findings)

    def test_no_finding_when_only_one_source_disagrees(self, conn):
        ticker = "SPOTCO2"
        d = date(2026, 6, 1)
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, ?, 100, 100, 100, 100, 1000)",
            [d, ticker],
        )
        fake_fy = _FakeFyers({ticker: pd.DataFrame([{"date": d.isoformat(), "close": 100.0}])})

        def fake_yahoo(t, dd):
            return 200.0

        findings = check_spot_check(
            conn, date(2026, 6, 5), sample_size=10, seed=1, fyers_client=fake_fy, yahoo_fetch=fake_yahoo
        )
        assert findings == []


class TestCheckCorporateActionsCoverage:
    def _insert_days(self, conn, ticker, start, n_days):
        for i in range(n_days):
            d = start + timedelta(days=i)
            conn.execute(
                "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) "
                "VALUES (?, ?, 100, 100, 100, 100, 1000)",
                [d, ticker],
            )

    def test_flags_ticker_with_no_corporate_actions(self, conn):
        as_of = date(2026, 6, 1)
        self._insert_days(conn, "NOACTIONS", date(2026, 1, 1), 5)

        findings = check_corporate_actions_coverage(conn, as_of, min_trading_days=3, lookback_years=10)

        assert len(findings) == 1
        assert findings[0].ticker == "NOACTIONS"
        assert findings[0].severity == "warning"

    def test_no_finding_when_ticker_has_a_corporate_action(self, conn):
        as_of = date(2026, 6, 1)
        self._insert_days(conn, "HASACTIONS", date(2026, 1, 1), 5)
        conn.execute(
            "INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio) VALUES (?, ?, 'DIVIDEND', 2.0)",
            ["HASACTIONS", date(2026, 3, 1)],
        )

        findings = check_corporate_actions_coverage(conn, as_of, min_trading_days=3, lookback_years=10)

        assert findings == []

    def test_no_finding_below_min_trading_days(self, conn):
        as_of = date(2026, 6, 1)
        self._insert_days(conn, "TOOFEWDAYS", date(2026, 1, 1), 2)

        findings = check_corporate_actions_coverage(conn, as_of, min_trading_days=3, lookback_years=10)

        assert findings == []

    def test_known_action_free_ticker_exempted(self, conn):
        as_of = date(2026, 6, 1)
        self._insert_days(conn, "NIFTYBEES", date(2026, 1, 1), 5)

        findings = check_corporate_actions_coverage(conn, as_of, min_trading_days=3, lookback_years=10)

        assert findings == []

    def test_flags_likely_rename_when_another_ticker_starts_right_after(self, conn):
        as_of = date(2026, 6, 1)
        self._insert_days(conn, "OLDNAME", date(2026, 1, 1), 5)
        # OLDNAME's last row is 2026-01-05; NEWNAME starts 2 days later —
        # the same signature TATAMOTORS -> TMPV showed.
        self._insert_days(conn, "NEWNAME", date(2026, 1, 7), 5)

        findings = check_corporate_actions_coverage(conn, as_of, min_trading_days=3, lookback_years=10)

        oldname_finding = next(f for f in findings if f.ticker == "OLDNAME")
        assert oldname_finding.evidence["likely_renamed_to"] == "NEWNAME"

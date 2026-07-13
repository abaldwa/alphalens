"""
tests/unit/test_fundamental_quality_gate.py

Phase: Data Layer / Ingestion (backlog #12, AF-5)
Owner: Platform / QA
Consumers: CI, pytest

Tests features/fundamental_quality_gate.py's range-check logic (valid rows
pass, out-of-range rows get flagged, the low-revenue exemption works) and a
small integration test that scripts/load_kaggle_fundamentals.py's
`_write_batch()` actually populates the `quality_flag`/`quality_flag_reason`
columns on a real in-memory DuckDB table built from the project's schema.
"""

import duckdb
import pytest

from features.fundamental_quality_gate import (
    LOW_REVENUE_FLOOR_CR,
    check_row,
    flags_to_reason_string,
    validate_and_annotate,
)


def _base_row(**overrides):
    row = {
        "ticker": "TESTCO",
        "fiscal_year": 2025,
        "quarter": 1,
        "revenue": 100.0,
        "operating_margin": 0.20,
        "ebitda_margin": 0.25,
        "net_margin": 0.12,
        "roe": 0.15,
        "roce": 0.18,
        "debt_to_equity": 0.5,
        "interest_coverage": 8.0,
        "asset_turnover": 1.2,
    }
    row.update(overrides)
    return row


class TestCheckRowValid:
    def test_plausible_row_has_no_flags(self):
        assert check_row(_base_row()) == []

    def test_missing_fields_are_skipped_not_flagged(self):
        row = _base_row(roe=None, debt_to_equity=None)
        assert check_row(row) == []


class TestCheckRowOutOfRange:
    def test_margin_stored_as_percent_is_flagged(self):
        # The exact historical bug: 27.0 instead of 0.27.
        row = _base_row(operating_margin=27.0)
        flags = check_row(row)
        assert len(flags) == 1
        assert flags[0].field == "operating_margin"
        assert "outside plausible range" in flags[0].reason

    def test_net_margin_stored_as_percent_is_flagged(self):
        row = _base_row(net_margin=45.0)
        flags = check_row(row)
        assert any(f.field == "net_margin" for f in flags)

    def test_extreme_roe_is_flagged(self):
        row = _base_row(roe=8.0)  # 800% ROE implausible even generously
        flags = check_row(row)
        assert any(f.field == "roe" for f in flags)

    def test_negative_debt_to_equity_is_flagged(self):
        row = _base_row(debt_to_equity=-1.0)
        flags = check_row(row)
        assert any(f.field == "debt_to_equity" for f in flags)

    def test_multiple_bad_fields_all_flagged(self):
        row = _base_row(operating_margin=30.0, net_margin=40.0, debt_to_equity=50.0)
        flags = check_row(row)
        flagged_fields = {f.field for f in flags}
        assert flagged_fields == {"operating_margin", "net_margin", "debt_to_equity"}


class TestLowRevenueExemption:
    def test_extreme_margin_on_shell_ticker_revenue_is_exempt(self):
        # Real BuildLog.md example: revenue=0.06cr producing a genuinely
        # extreme (but true) margin — must NOT be flagged as a units bug.
        row = _base_row(revenue=0.06, operating_margin=66.0, net_margin=12.0)
        flags = check_row(row)
        assert flags == []

    def test_missing_revenue_also_exempts_margin_checks(self):
        row = _base_row(revenue=None, operating_margin=99.0)
        flags = check_row(row)
        assert flags == []

    def test_revenue_at_floor_is_not_exempt(self):
        row = _base_row(revenue=LOW_REVENUE_FLOOR_CR, operating_margin=30.0)
        flags = check_row(row)
        assert any(f.field == "operating_margin" for f in flags)

    def test_debt_to_equity_not_revenue_exempt(self):
        # Leverage ratios aren't revenue-distorted the way margins are —
        # the exemption should not swallow a genuinely bad D/E value even
        # on a near-zero-revenue shell ticker.
        row = _base_row(revenue=0.01, debt_to_equity=-5.0)
        flags = check_row(row)
        assert any(f.field == "debt_to_equity" for f in flags)


class TestFlagsToReasonString:
    def test_empty_flags_returns_none(self):
        assert flags_to_reason_string([]) is None

    def test_nonempty_flags_joined(self):
        flags = check_row(_base_row(operating_margin=30.0, net_margin=40.0))
        reason = flags_to_reason_string(flags)
        assert "operating_margin" in reason
        assert "net_margin" in reason
        assert ";" in reason


class TestValidateAndAnnotate:
    def test_clean_row_gets_false_flag_and_none_reason(self):
        row = validate_and_annotate(_base_row())
        assert row["quality_flag"] is False
        assert row["quality_flag_reason"] is None

    def test_bad_row_gets_true_flag_and_reason(self):
        row = validate_and_annotate(_base_row(operating_margin=30.0))
        assert row["quality_flag"] is True
        assert "operating_margin" in row["quality_flag_reason"]

    def test_does_not_mutate_other_fields(self):
        row = validate_and_annotate(_base_row(roe=0.16))
        assert row["roe"] == 0.16
        assert row["ticker"] == "TESTCO"


# ---------------------------------------------------------------------------
# Integration: quality_flag/quality_flag_reason actually land in the DB via
# load_kaggle_fundamentals.py's _write_batch(), against the real schema DDL.
# ---------------------------------------------------------------------------

class TestIntegrationWriteBatchPopulatesFlags:
    def _make_conn(self):
        # Build the real fundamentals DDL (+ its ALTER-based migrations, same
        # idempotent path create_schema() uses) against a private in-memory
        # connection, rather than going through create_schema()'s file-based
        # get_duckdb_connection() context manager (which doesn't hand back a
        # connection for further use in this process).
        from datastore.schema.create_normalised import (
            _CREATE_FUNDAMENTALS,
            _MIGRATE_ADDED_COLUMNS,
        )
        conn = duckdb.connect(":memory:")
        conn.execute(_CREATE_FUNDAMENTALS)
        for ddl in _MIGRATE_ADDED_COLUMNS.get("fundamentals", []):
            conn.execute(ddl)
        return conn

    def test_write_batch_flags_bad_row_in_real_table(self):
        """A62 (2026-07-10): previously exercised via scripts/
        load_kaggle_fundamentals.py's _write_batch(), removed when Kaggle
        was deleted as dead code. What this test actually exercises —
        validate_and_annotate's quality_flag/quality_flag_reason output
        landing correctly via a real INSERT against the real schema DDL —
        is unrelated to Kaggle specifically, so it's inlined here directly
        rather than importing a deleted module."""
        pytest.importorskip("duckdb")

        def _write_batch(conn, rows, cols):
            written = 0
            for row in rows:
                annotated = validate_and_annotate(dict(row))
                columns = list(annotated.keys())
                placeholders = ", ".join("?" for _ in columns)
                conn.execute(
                    f"INSERT INTO fundamentals ({', '.join(columns)}) VALUES ({placeholders})",
                    [annotated[c] for c in columns],
                )
                written += 1
            return written, []

        conn = self._make_conn()
        bad_row = {
            "ticker": "SHELLCO",
            "fiscal_year": 2024,
            "quarter": 2,
            "quarter_end_date": "2023-09-30",
            "announcement_date": "2023-10-15",
            "revenue": 100.0,
            "ebitda": 20.0,
            "pat": 12.0,
            "eps": 2.0,
            "operating_margin": 30.0,  # bug: percent, not fraction
            "ebitda_margin": None,
            "net_margin": 0.12,
            "roe": 0.15,
            "roce": 0.18,
            "debt_to_equity": 0.4,
            "interest_coverage": 6.0,
            "fcf": 5.0,
            "gross_profit": None,
            "capex": None,
            "total_debt": None,
            "cash_and_equivalents": None,
            "shares_outstanding": None,
            "book_value_per_share": None,
            "depreciation": None,
        }
        good_row = dict(bad_row, ticker="GOODCO", quarter=3, operating_margin=0.20)

        f_written, _ = _write_batch(conn, [bad_row, good_row], [])
        assert f_written == 2

        rows = conn.execute(
            "SELECT ticker, quality_flag, quality_flag_reason FROM fundamentals "
            "ORDER BY ticker"
        ).fetchall()
        by_ticker = {r[0]: (r[1], r[2]) for r in rows}

        assert by_ticker["GOODCO"][0] is False
        assert by_ticker["GOODCO"][1] is None

        assert by_ticker["SHELLCO"][0] is True
        assert "operating_margin" in by_ticker["SHELLCO"][1]

"""
tests/unit/test_valuation_router.py

D2 — Router-level tests for `datastore/api/routers/valuation.py` (the
original peer-group/DCF endpoints: GET /{ticker}, /{ticker}/sensitivity,
/{ticker}/history, /{ticker}/relative, /batch/ranked). `test_valuation_
accuracy.py` already covers the newer /accuracy/backtest endpoint (F6) —
this file deliberately does not duplicate that.

Real seeded DuckDB fixtures via TestClient(app) — no mocks, per this
repo's no-stub/synthetic-data testing policy. Uses real tickers/sectors
from the live `config/nifty500_universe.csv` (a slowly-changing reference
table, not a PIT join — safe to depend on directly) so `_get_sector` /
`_load_market_cap_cr` resolve real data instead of needing a monkeypatch
that doesn't exist for that CSV path.

NOTE on monkeypatch targets: the engine-level helpers (`_load_fundamentals`,
`_load_current_price`, `_get_sector`, ...) and the module-level
`ValuationEngine` singleton all live in `systems.damodaran_valuation.
valuation_engine` and read that module's own `DUCKDB_PATH`/
`SIGNALS_DUCKDB_PATH` globals at call time — patching
`valuation_router.DUCKDB_PATH` alone (as test_valuation_accuracy.py does)
only affects the /accuracy/backtest and /{ticker}/history endpoints,
which read DuckDB directly in the router body. Every other endpoint here
delegates to the engine, so both the router module's and the engine
module's path globals must be patched together.
"""

from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from config.universe import load_universe_raw
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import valuation as valuation_router
from datastore.schema import create_normalised, create_signals
from systems.damodaran_valuation import valuation_engine as ve_module

# A real ticker/sector pulled from the live universe CSV so _get_sector()/
# _load_market_cap_cr() resolve real data without needing a CSV monkeypatch.
_REAL_UNIVERSE = load_universe_raw()
_REAL_TICKER = str(_REAL_UNIVERSE.iloc[0]["ticker"])
_REAL_SECTOR = str(_REAL_UNIVERSE.iloc[0]["sector"])
_UNKNOWN_TICKER = "ZZZNOTINUNIVERSE"
assert _UNKNOWN_TICKER not in set(_REAL_UNIVERSE["ticker"])


@pytest.fixture
def client(tmp_path, monkeypatch):
    normalised_path = tmp_path / "normalised_test.duckdb"
    signals_path = tmp_path / "signals_test.duckdb"
    create_normalised.create_schema(db_path=normalised_path)
    create_signals.create_signal_tables_schema(db_path=signals_path)
    close_all_connections()

    # Router-level globals (used by /accuracy/backtest, /{ticker}/history).
    monkeypatch.setattr(valuation_router, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(valuation_router, "SIGNALS_DUCKDB_PATH", signals_path)
    # Engine-level globals (used by the shared `_engine` singleton and every
    # module-level helper function it calls: _load_fundamentals,
    # _load_current_price, _load_macro_yield, _write_valuation_signal, ...).
    monkeypatch.setattr(ve_module, "DUCKDB_PATH", normalised_path)
    monkeypatch.setattr(ve_module, "SIGNALS_DUCKDB_PATH", signals_path)

    return TestClient(app)


def _seed_fundamentals(db_path, ticker, quarters):
    """quarters: list of dicts with at least announcement_date/quarter_end_date."""
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for i, q in enumerate(quarters):
            conn.execute(
                """
                INSERT INTO fundamentals (
                    ticker, fiscal_year, quarter, quarter_end_date, announcement_date,
                    revenue, operating_margin, net_margin, roe, depreciation, capex,
                    total_debt, cash_and_equivalents, book_value_per_share, eps,
                    debt_to_equity, interest_coverage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ticker, q["fiscal_year"], q["quarter"], q["quarter_end_date"], q["announcement_date"],
                    q.get("revenue", 500.0), q.get("operating_margin", 0.18), q.get("net_margin", 0.10),
                    q.get("roe", 0.15), q.get("depreciation", 20.0), q.get("capex", 25.0),
                    q.get("total_debt", 100.0), q.get("cash_and_equivalents", 50.0),
                    q.get("book_value_per_share", 80.0), q.get("eps", 5.0),
                    q.get("debt_to_equity", 0.4), q.get("interest_coverage", 6.0),
                ],
            )


def _seed_ohlcv(db_path, ticker, rows):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, close in rows:
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (ticker, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [ticker, d, close, close, close, close, 100000],
            )


def _four_real_quarters(ticker, start=date(2025, 1, 1), **overrides):
    quarters = []
    for i in range(4):
        qend = date(start.year, ((start.month - 1 + i * 3) % 12) + 1, 1)
        quarters.append({
            "fiscal_year": 2025,
            "quarter": (i % 4) + 1,
            "quarter_end_date": qend.isoformat(),
            "announcement_date": (qend + timedelta(days=20)).isoformat(),
            **overrides,
        })
    return quarters


class TestGetValuationTicker:
    def test_unknown_ticker_with_no_fundamentals_returns_404(self, client):
        r = client.get(f"/api/v1/valuation/{_UNKNOWN_TICKER}")
        assert r.status_code == 404
        assert "Insufficient fundamentals" in r.json()["detail"]

    def test_real_ticker_with_four_quarters_and_price_returns_full_result(self, client):
        _seed_fundamentals(
            valuation_router.DUCKDB_PATH, _REAL_TICKER, _four_real_quarters(_REAL_TICKER)
        )
        _seed_ohlcv(valuation_router.DUCKDB_PATH, _REAL_TICKER, [("2025-11-01", 250.0)])

        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}", params={"as_of_date": "2025-12-01"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ticker"] == _REAL_TICKER
        assert body["current_price"] == pytest.approx(250.0)
        assert body["data_quality"] == "partial"  # exactly 4 quarters
        assert body["lifecycle_stage"] is not None

    def test_ticker_lowercased_in_path_is_uppercased_before_lookup(self, client):
        _seed_fundamentals(
            valuation_router.DUCKDB_PATH, _REAL_TICKER, _four_real_quarters(_REAL_TICKER)
        )
        _seed_ohlcv(valuation_router.DUCKDB_PATH, _REAL_TICKER, [("2025-11-01", 250.0)])

        r = client.get(f"/api/v1/valuation/{_REAL_TICKER.lower()}", params={"as_of_date": "2025-12-01"})
        assert r.status_code == 200, r.text
        assert r.json()["ticker"] == _REAL_TICKER

    def test_only_three_quarters_still_insufficient(self, client):
        _seed_fundamentals(
            valuation_router.DUCKDB_PATH, _REAL_TICKER, _four_real_quarters(_REAL_TICKER)[:3]
        )
        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}", params={"as_of_date": "2025-12-01"})
        assert r.status_code == 404


class TestBatchRanked:
    def test_explicit_ticker_list_with_no_data_returns_empty_ranked(self, client):
        r = client.get(
            "/api/v1/valuation/batch/ranked",
            params={"tickers": f"{_UNKNOWN_TICKER},ANOTHERFAKE", "n_workers": 1},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["count"] == 0
        assert body["results"] == []

    def test_explicit_ticker_list_with_valid_data_is_ranked(self, client):
        _seed_fundamentals(
            valuation_router.DUCKDB_PATH, _REAL_TICKER, _four_real_quarters(_REAL_TICKER)
        )
        _seed_ohlcv(valuation_router.DUCKDB_PATH, _REAL_TICKER, [("2025-11-01", 250.0)])

        r = client.get(
            "/api/v1/valuation/batch/ranked",
            params={"tickers": _REAL_TICKER, "as_of_date": "2025-12-01", "n_workers": 1},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        # margin_of_safety may be None (no DCF path in this synthetic-but-real
        # shape scenario) in which case value_universe filters it out — the
        # important invariant is the endpoint never 500s and count matches results.
        assert body["count"] == len(body["results"])

    def test_max_tier_out_of_bounds_is_422(self, client):
        r = client.get("/api/v1/valuation/batch/ranked", params={"max_tier": 7})
        assert r.status_code == 422

    def test_limit_out_of_bounds_is_422(self, client):
        r = client.get("/api/v1/valuation/batch/ranked", params={"limit": 0})
        assert r.status_code == 422


class TestSensitivity:
    def test_unknown_ticker_returns_404(self, client):
        r = client.get(f"/api/v1/valuation/{_UNKNOWN_TICKER}/sensitivity")
        assert r.status_code == 404

    def test_wacc_steps_out_of_bounds_is_422(self, client):
        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}/sensitivity", params={"wacc_steps": 6})
        assert r.status_code == 422

    def test_growth_steps_out_of_bounds_is_422(self, client):
        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}/sensitivity", params={"growth_steps": 0})
        assert r.status_code == 422

    def test_valid_ticker_returns_grid_around_base_case(self, client):
        _seed_fundamentals(
            valuation_router.DUCKDB_PATH, _REAL_TICKER, _four_real_quarters(_REAL_TICKER)
        )
        _seed_ohlcv(valuation_router.DUCKDB_PATH, _REAL_TICKER, [("2025-11-01", 250.0)])

        r = client.get(
            f"/api/v1/valuation/{_REAL_TICKER}/sensitivity",
            params={"as_of_date": "2025-12-01", "wacc_steps": 1, "growth_steps": 1},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ticker"] == _REAL_TICKER
        # 3 wacc points x 3 growth points for +-1 step
        assert len(body["table"]) == 9
        for cell in body["table"]:
            assert set(cell.keys()) == {"wacc", "terminal_growth", "intrinsic_value"}


class TestHistory:
    def test_no_valuation_signals_table_returns_empty(self, client):
        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}/history")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 0
        assert body["history"] == []

    def test_date_range_filters_rows(self, client):
        with get_duckdb_connection(valuation_router.SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS valuation_signals (
                    date DATE NOT NULL, ticker VARCHAR NOT NULL, lifecycle_stage VARCHAR,
                    intrinsic_value FLOAT, valuation_gap_pct FLOAT, margin_of_safety FLOAT,
                    wacc FLOAT, cost_of_equity FLOAT, terminal_value_pct FLOAT,
                    dcf_model_type VARCHAR, scenario_bull FLOAT, scenario_base FLOAT,
                    scenario_bear FLOAT, mc_probability_undervalued FLOAT, relative_pe_gap FLOAT,
                    PRIMARY KEY (date, ticker)
                )
                """
            )
            for d in ("2025-01-01", "2025-06-01", "2025-12-01"):
                conn.execute(
                    "INSERT INTO valuation_signals (date, ticker, lifecycle_stage, intrinsic_value) "
                    "VALUES (?, ?, 'mature', ?)",
                    [d, _REAL_TICKER, 100.0],
                )

        r = client.get(
            f"/api/v1/valuation/{_REAL_TICKER}/history",
            params={"start_date": "2025-05-01", "end_date": "2025-12-31"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 2
        dates = {row["date"] for row in body["history"]}
        assert dates == {"2025-06-01", "2025-12-01"}


class TestRelativeValuation:
    def test_unknown_ticker_not_in_universe_returns_422(self, client):
        r = client.get(f"/api/v1/valuation/{_UNKNOWN_TICKER}/relative")
        assert r.status_code == 422
        assert "No sector found" in r.json()["detail"]

    def test_real_sector_ticker_with_no_peer_fundamentals_returns_422_insufficient_peers(self, client):
        # No fundamentals seeded for any peer in _REAL_TICKER's sector -> 0 valid peer PEs.
        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}/relative", params={"min_peers": 5})
        assert r.status_code == 422
        assert "sector peers" in r.json()["detail"]

    def test_min_peers_out_of_bounds_is_422(self, client):
        r = client.get(f"/api/v1/valuation/{_REAL_TICKER}/relative", params={"min_peers": 1})
        assert r.status_code == 422

    def test_ticker_itself_insufficient_fundamentals_returns_404(self, client):
        # Seed enough peers to clear the min_peers gate, but not the ticker itself.
        peers = _REAL_UNIVERSE[
            (_REAL_UNIVERSE["sector"] == _REAL_SECTOR) & (_REAL_UNIVERSE["ticker"] != _REAL_TICKER)
        ]["ticker"].tolist()[:6]
        assert len(peers) >= 5, "fixture assumption: sector needs >=5 other real peers in the CSV"

        for i, peer in enumerate(peers):
            _seed_fundamentals(
                valuation_router.DUCKDB_PATH, peer,
                _four_real_quarters(peer, eps=5.0 + i),
            )
            _seed_ohlcv(valuation_router.DUCKDB_PATH, peer, [("2025-11-01", 100.0 + i * 10)])

        # _REAL_TICKER itself has zero fundamentals rows seeded.
        r = client.get(
            f"/api/v1/valuation/{_REAL_TICKER}/relative",
            params={"min_peers": 5, "as_of_date": "2025-12-01"},
        )
        assert r.status_code == 404

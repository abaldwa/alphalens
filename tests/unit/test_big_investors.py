"""
tests/unit/test_big_investors.py

BI4 — real automated coverage for Big Investor Activity logic that
previously had none (see FeatureBacklog.md's BI4 writeup): the family
position/WAC replay in `_position_and_wac_asof`
(datastore/api/routers/big_investors.py), `_parse_bulk_block_deals_table`
(ingestion/scrapers/trendlyne.py), `backfill_bulk_deals_history`'s
NOT EXISTS anti-join dedup, and the MF Holdings movers'
`scheme_count_change` computation.

Also covers BI6's fuzzy "unmapped:" family <-> Trendlyne holder-name
matching (`_fuzzy_match_unmapped_family` / `_is_positional_abbreviation_match`
/ `_token_jaccard`), including near-miss true positives and genuinely
different real investors as false-positive guards.

Uses a real seeded DuckDB fixture (datastore/schema/create_normalised.py's
real schema, a temp file-backed DB) per this project's no-stub/synthetic-
data testing policy — no mocks of the DB layer.
"""

from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.api.main import app
from datastore.api.routers import big_investors as bi_router
from datastore.schema import create_normalised
from ingestion.scrapers.trendlyne import _parse_bulk_block_deals_table


# ===== Fixtures =====

@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "big_investors_test.duckdb"
    create_normalised.create_schema(db_path=path)
    close_all_connections()
    return path


@pytest.fixture
def client(db_path, monkeypatch):
    monkeypatch.setattr(bi_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _seed_stock_master(conn, ticker, company_name, market_cap_cr):
    conn.execute(
        """
        INSERT INTO stock_master (ticker, company_name, sector, industry, nse_series, market_cap_cr,
                                   current_tier, is_fno_eligible, is_nifty500)
        VALUES (?, ?, 'Sector', 'Industry', 'EQ', ?, 1, FALSE, TRUE)
        ON CONFLICT DO NOTHING
        """,
        [ticker, company_name, market_cap_cr],
    )


def _seed_ohlcv(conn, ticker, rows):
    for d, close in rows:
        conn.execute(
            """
            INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, 100000)
            ON CONFLICT DO NOTHING
            """,
            [d, ticker, close, close, close, close],
        )


def _seed_bulk_deal_position(conn, family_id, ticker, trade_date, net_type, net_qty, avg_price,
                              deal_type="BULK", cumulative=None, is_new_entry=False):
    conn.execute(
        """
        INSERT INTO bulk_deal_positions
            (family_id, ticker, trade_date, deal_type, net_transaction_type, net_quantity,
             avg_price, exchange, cumulative_position_est, is_new_entry, is_full_exit)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'NSE', ?, ?, FALSE)
        ON CONFLICT DO NOTHING
        """,
        [family_id, ticker, trade_date, deal_type, net_type, net_qty, avg_price,
         cumulative if cumulative is not None else net_qty, is_new_entry],
    )


def _seed_public_shareholder(conn, ticker, holder_name, quarter_end, reported_shares, family_id=None, stake_pct=None):
    conn.execute(
        """
        INSERT INTO public_shareholders
            (ticker, holder_name, quarter_end_date, filing_date, family_id, stake_pct,
             qoq_change_pct, reported_shares, source, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'trendlyne', ?)
        ON CONFLICT DO NOTHING
        """,
        [ticker, holder_name, quarter_end, quarter_end, family_id, stake_pct, reported_shares, datetime.utcnow()],
    )


def _seed_mf_holdings(conn, ticker, month, scheme_qty, availability_date):
    """scheme_qty: list of (scheme_name, quantity, value_inr)."""
    for scheme_name, qty, value_inr in scheme_qty:
        conn.execute(
            """
            INSERT INTO mf_holdings (ticker, month, scheme_name, isin, quantity, value_inr, availability_date)
            VALUES (?, ?, ?, NULL, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [ticker, month, scheme_name, qty, value_inr, availability_date],
        )


# ===== _position_and_wac_asof: bulk-deal + Trendlyne replay =====

class TestPositionAndWacAsof:
    def test_simple_buy_tracks_qty_and_wac(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 1, 5), "BUY", 1000, 100.0)
            result = bi_router._position_and_wac_asof(conn, [("FAM1", "TICK1")])
        qty, wac, trendlyne_positive = result[("FAM1", "TICK1", date(2026, 1, 5))]
        assert qty == 1000
        assert wac == 100.0
        assert trendlyne_positive is False

    def test_sell_draws_down_qty_without_changing_wac(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 1, 5), "BUY", 1000, 100.0)
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 1, 10), "SELL", 400, 150.0)
            result = bi_router._position_and_wac_asof(conn, [("FAM1", "TICK1")])
        qty, wac, _ = result[("FAM1", "TICK1", date(2026, 1, 10))]
        assert qty == 600
        # WAC of what's LEFT stays at the original buy cost, not the sale price.
        assert wac == pytest.approx(100.0)

    # NOTE: _position_and_wac_asof only ever writes a `result` entry while
    # replaying a *trade* event (bulk_deal_positions row) — a Trendlyne
    # checkpoint updates the running qty/cost in place but does not itself
    # add a result entry (only the `if payload[0] == "trade":` branch does
    # `result[...] = ...`). So the tests below seed one more small trade
    # AFTER the checkpoint date to observe the checkpoint's effect through
    # that later result row.

    def test_trendlyne_checkpoint_trues_down_undisclosed_sale(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 1, 5), "BUY", 1000, 100.0)
            # Trendlyne's quarterly filing shows fewer shares than bulk-deal data tracked
            # -> an undisclosed sale (below the 0.5% disclosure threshold) is inferred.
            _seed_public_shareholder(conn, "TICK1", "FAM1 HOLDER", date(2026, 3, 31), 600, family_id="FAM1")
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 4, 1), "BUY", 100, 90.0)
            result = bi_router._position_and_wac_asof(conn, [("FAM1", "TICK1")])
        qty, wac, _ = result[("FAM1", "TICK1", date(2026, 4, 1))]
        # 600 (trued-down remainder, WAC still 100 unchanged by a sale) + 100 new @ 90.
        assert qty == 700
        expected_cost = 600 * 100.0 + 100 * 90.0
        assert wac == pytest.approx(expected_cost / 700)

    def test_trendlyne_checkpoint_trues_up_undisclosed_purchase_at_nearest_close(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 1, 5), "BUY", 1000, 100.0)
            _seed_ohlcv(conn, "TICK1", [(date(2026, 3, 30), 120.0), (date(2026, 3, 31), 125.0)])
            # Trendlyne shows MORE shares than tracked -> an undisclosed purchase, costed
            # at the nearest OHLCV close on/before the checkpoint date (not the checkpoint date itself
            # if that date has no print - 2026-03-31 does, so it's used directly here).
            _seed_public_shareholder(conn, "TICK1", "FAM1 HOLDER", date(2026, 3, 31), 1500, family_id="FAM1")
            _seed_bulk_deal_position(conn, "FAM1", "TICK1", date(2026, 4, 1), "SELL", 100, 130.0)
            result = bi_router._position_and_wac_asof(conn, [("FAM1", "TICK1")])
        qty, wac, trendlyne_positive = result[("FAM1", "TICK1", date(2026, 4, 1))]
        assert qty == 1400  # 1500 trued-up then a 100-share sale
        assert trendlyne_positive is True
        wac_after_checkpoint = (1000 * 100.0 + 500 * 125.0) / 1500
        assert wac == pytest.approx(wac_after_checkpoint)  # a sale doesn't change WAC

    def test_unmapped_family_exact_normalization_match(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "unmapped:JOHN DOE", "TICK1", date(2026, 1, 5), "BUY", 500, 50.0)
            # family_id NULL on the public_shareholders row -> matched via
            # re-normalization of holder_name, per _position_and_wac_asof's docstring.
            _seed_public_shareholder(conn, "TICK1", "John Doe", date(2026, 3, 31), 800, family_id=None)
            _seed_bulk_deal_position(conn, "unmapped:JOHN DOE", "TICK1", date(2026, 4, 1), "BUY", 50, 60.0)
            result = bi_router._position_and_wac_asof(conn, [("unmapped:JOHN DOE", "TICK1")])
        qty, wac, trendlyne_positive = result[("unmapped:JOHN DOE", "TICK1", date(2026, 4, 1))]
        assert qty == 850  # 800 (trued up by the exact-match checkpoint) + 50 new
        assert trendlyne_positive is True

    def test_unmapped_family_fuzzy_match_catches_abbreviation(self, db_path):
        """BI6: 'HITESH R JAVERI' (holder_name on Trendlyne) should cross-check
        against the bulk-deal-derived 'unmapped:HITESH RAMJI JAVERI' family
        even though exact re-normalization doesn't match."""
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "unmapped:HITESH RAMJI JAVERI", "TICK1", date(2026, 1, 5), "BUY", 700, 80.0)
            _seed_public_shareholder(conn, "TICK1", "Hitesh R Javeri", date(2026, 3, 31), 900, family_id=None)
            _seed_bulk_deal_position(conn, "unmapped:HITESH RAMJI JAVERI", "TICK1", date(2026, 4, 1), "BUY", 10, 95.0)
            result = bi_router._position_and_wac_asof(conn, [("unmapped:HITESH RAMJI JAVERI", "TICK1")])
        # If the fuzzy match hadn't fired, the checkpoint's true-up to 900 would
        # never have happened and this would be 710 (700 + 10), not 910.
        qty, wac, trendlyne_positive = result[("unmapped:HITESH RAMJI JAVERI", "TICK1", date(2026, 4, 1))]
        assert qty == 910
        assert trendlyne_positive is True

    def test_unmapped_family_fuzzy_match_does_not_merge_different_investors(self, db_path):
        """BI6 false-positive guard: 'Ashok Kacholia' (a different real name)
        must NOT be fuzzily matched onto 'unmapped:ASHISH KACHOLIA'."""
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_bulk_deal_position(conn, "unmapped:ASHISH KACHOLIA", "TICK1", date(2026, 1, 5), "BUY", 300, 60.0)
            _seed_public_shareholder(conn, "TICK1", "Ashok Kacholia", date(2026, 3, 31), 999, family_id=None)
            result = bi_router._position_and_wac_asof(conn, [("unmapped:ASHISH KACHOLIA", "TICK1")])
        # No checkpoint event should have been attached to this family/ticker.
        qty, wac, trendlyne_positive = result[("unmapped:ASHISH KACHOLIA", "TICK1", date(2026, 1, 5))]
        assert qty == 300
        assert trendlyne_positive is False


class TestFuzzyMatchUnmappedFamily:
    """Direct unit coverage of the BI6 heuristic, isolated from the DB replay."""

    def test_and_associates_suffix_matches_via_jaccard(self):
        got = bi_router._fuzzy_match_unmapped_family(
            "SHARAD KANAYALAL SHAH", ["unmapped:SHARAD KANAYALAL SHAH AND ASSOCIATES"]
        )
        assert got == "unmapped:SHARAD KANAYALAL SHAH AND ASSOCIATES"

    def test_reordered_tokens_match_via_jaccard(self):
        got = bi_router._fuzzy_match_unmapped_family(
            "SHAH SHARAD KANAYALAL", ["unmapped:SHARAD KANAYALAL SHAH"]
        )
        assert got == "unmapped:SHARAD KANAYALAL SHAH"

    def test_abbreviated_middle_name_matches_positionally(self):
        got = bi_router._fuzzy_match_unmapped_family(
            "HITESH R JAVERI", ["unmapped:HITESH RAMJI JAVERI"]
        )
        assert got == "unmapped:HITESH RAMJI JAVERI"

    def test_similar_but_different_surname_first_name_not_matched(self):
        # "Ashish" vs "Ashok" — high edit-distance similarity but NOT a prefix
        # relationship and NOT a token-Jaccard hit; must not match.
        got = bi_router._fuzzy_match_unmapped_family(
            "ASHISH KACHOLIA", ["unmapped:ASHOK KACHOLIA"]
        )
        assert got is None

    def test_unrelated_surname_sharing_one_token_not_matched(self):
        got = bi_router._fuzzy_match_unmapped_family(
            "AKASH BHANSHALI", ["unmapped:LATA BHANSHALI"]
        )
        assert got is None

    def test_ambiguous_multi_candidate_match_returns_none(self):
        # Two candidates both plausibly close -> treated as ambiguous, not a guess.
        got = bi_router._fuzzy_match_unmapped_family(
            "A B SHAH", ["unmapped:A B SHAH AND ASSOCIATES", "unmapped:A B SHAH FAMILY"]
        )
        assert got is None

    def test_exact_match_short_circuits(self):
        got = bi_router._fuzzy_match_unmapped_family(
            "DOLLY KHANNA", ["unmapped:DOLLY KHANNA"]
        )
        assert got == "unmapped:DOLLY KHANNA"

    def test_no_candidates_returns_none(self):
        assert bi_router._fuzzy_match_unmapped_family("ANYONE", []) is None


# ===== _parse_bulk_block_deals_table =====

class TestParseBulkBlockDealsTable:
    _TABLE_HTML = """
    <html><body>
    <table id="bbdealTable">
      <tbody>
        <tr>
          <td data-export="Tata Consultancy Services">TCS</td>
          <td>Rakesh Jhunjhunwala and Associates</td>
          <td>NSE</td>
          <td>BULK</td>
          <td>Purchase</td>
          <td data-order="2026-05-14">14 May 2026</td>
          <td>3500.50</td>
          <td>10,000</td>
        </tr>
        <tr>
          <td data-export="Infosys Ltd">INFY</td>
          <td>Rakesh Jhunjhunwala and Associates</td>
          <td>BSE</td>
          <td>BLOCK</td>
          <td>Sell</td>
          <td data-order="2026-06-01">01 Jun 2026</td>
          <td>-</td>
          <td>5,000</td>
        </tr>
      </tbody>
    </table>
    </body></html>
    """

    def test_parses_real_row_shape(self):
        rows = _parse_bulk_block_deals_table(self._TABLE_HTML)
        assert len(rows) == 2
        first = rows[0]
        assert first["company_name"] == "Tata Consultancy Services"
        assert first["exchange"] == "NSE"
        assert first["deal_type"] == "BULK"
        assert first["trade_date"] == "2026-05-14"
        assert first["price"] == 3500.50
        assert first["quantity"] == 10000

    def test_dash_price_parses_to_none_not_zero(self):
        rows = _parse_bulk_block_deals_table(self._TABLE_HTML)
        assert rows[1]["price"] is None

    def test_missing_table_returns_empty_list_not_raise(self):
        assert _parse_bulk_block_deals_table("<html><body>no deals table here</body></html>") == []

    def test_row_with_too_few_cells_is_skipped(self):
        html = """
        <table id="bbdealTable"><tbody>
          <tr><td>OnlyOne</td></tr>
        </tbody></table>
        """
        assert _parse_bulk_block_deals_table(html) == []

    def test_row_missing_company_name_or_date_is_skipped(self):
        html = """
        <table id="bbdealTable"><tbody>
          <tr>
            <td data-export="">Blank</td>
            <td>Client</td><td>NSE</td><td>BULK</td><td>Purchase</td>
            <td data-order="">no date</td><td>100</td><td>10</td>
          </tr>
        </tbody></table>
        """
        assert _parse_bulk_block_deals_table(html) == []


# ===== backfill_bulk_deals_history dedup anti-join =====

class TestBackfillBulkDealsHistoryDedup:
    """
    backfill_bulk_deals_history() itself calls export_bulk_deals_history()
    (a live network fetch across all 62 investors) — not something a unit
    test should call. Instead, this exercises the dedup anti-join SQL
    directly (the actual logic under test, per BI4), the same INSERT ...
    WHERE NOT EXISTS statement backfill_bulk_deals_history runs, against a
    real seeded large_deals table.
    """

    _INSERT_SQL = """
        INSERT INTO large_deals
            (trade_date, exchange, deal_type, ticker,
             client_name, transaction_type, quantity, price, remarks)
        SELECT
            CAST(s.trade_date AS DATE), s.exchange, s.deal_type, s.ticker,
            s.client_name, s.transaction_type,
            CAST(s.quantity AS BIGINT), CAST(s.price AS DOUBLE), s.remarks
        FROM _staging s
        WHERE NOT EXISTS (
            SELECT 1 FROM large_deals ld
            WHERE ld.trade_date = CAST(s.trade_date AS DATE)
              AND ld.exchange = s.exchange
              AND ld.deal_type = s.deal_type
              AND ld.ticker = s.ticker
              AND ld.client_name IS NOT DISTINCT FROM s.client_name
              AND ld.transaction_type IS NOT DISTINCT FROM s.transaction_type
              AND ld.quantity IS NOT DISTINCT FROM CAST(s.quantity AS BIGINT)
              AND ld.price IS NOT DISTINCT FROM CAST(s.price AS DOUBLE)
        )
    """

    def test_new_row_is_inserted(self, db_path):
        import pandas as pd

        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            df = pd.DataFrame([{
                "trade_date": "2026-05-14", "exchange": "NSE", "deal_type": "BULK", "ticker": "TCS",
                "client_name": "Rakesh Jhunjhunwala and Associates", "transaction_type": "BUY",
                "quantity": 10000, "price": 3500.5, "remarks": "trendlyne:Rakesh Jhunjhunwala and Associates",
            }])
            conn.register("_staging", df)
            conn.execute(self._INSERT_SQL)
            conn.unregister("_staging")
            count = conn.execute("SELECT COUNT(*) FROM large_deals").fetchone()[0]
        assert count == 1

    def test_exact_duplicate_row_is_not_reinserted(self, db_path):
        import pandas as pd

        row = {
            "trade_date": "2026-05-14", "exchange": "NSE", "deal_type": "BULK", "ticker": "TCS",
            "client_name": "Rakesh Jhunjhunwala and Associates", "transaction_type": "BUY",
            "quantity": 10000, "price": 3500.5, "remarks": "trendlyne:Rakesh Jhunjhunwala and Associates",
        }
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO large_deals (trade_date, exchange, deal_type, ticker, client_name, "
                "transaction_type, quantity, price, remarks) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [date(2026, 5, 14), row["exchange"], row["deal_type"], row["ticker"], row["client_name"],
                 row["transaction_type"], row["quantity"], row["price"], "nse_direct"],
            )
            df = pd.DataFrame([row])
            conn.register("_staging", df)
            conn.execute(self._INSERT_SQL)
            conn.unregister("_staging")
            count = conn.execute("SELECT COUNT(*) FROM large_deals").fetchone()[0]
        # remarks differs (nse_direct vs trendlyne:...) but remarks is NOT part
        # of the dedup tuple by design (see backfill_bulk_deals_history's
        # docstring) -> the Trendlyne row is correctly treated as a duplicate
        # of the already-ingested NSE-direct row and skipped.
        assert count == 1

    def test_same_day_different_client_both_kept(self, db_path):
        import pandas as pd

        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO large_deals (trade_date, exchange, deal_type, ticker, client_name, "
                "transaction_type, quantity, price, remarks) VALUES (?, 'NSE', 'BULK', 'TCS', "
                "'Other Investor', 'BUY', 2000, 3400.0, 'nse_direct')",
                [date(2026, 5, 14)],
            )
            df = pd.DataFrame([{
                "trade_date": "2026-05-14", "exchange": "NSE", "deal_type": "BULK", "ticker": "TCS",
                "client_name": "Rakesh Jhunjhunwala and Associates", "transaction_type": "BUY",
                "quantity": 10000, "price": 3500.5, "remarks": "trendlyne:Rakesh Jhunjhunwala and Associates",
            }])
            conn.register("_staging", df)
            conn.execute(self._INSERT_SQL)
            conn.unregister("_staging")
            count = conn.execute("SELECT COUNT(*) FROM large_deals").fetchone()[0]
        assert count == 2


# ===== MF Holdings movers: scheme_count_change =====

class TestMfHoldingsMoversSchemeCountChange:
    def test_scheme_count_change_computed_correctly(self, client, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_stock_master(conn, "TICK1", "Tick One", 5000.0)
            # prev month: 2 schemes hold it; curr month: 3 schemes (one new entrant).
            _seed_mf_holdings(conn, "TICK1", date(2026, 5, 1),
                               [("Scheme A", 1000, 1_000_000.0), ("Scheme B", 500, 500_000.0)],
                               date(2026, 5, 15))
            _seed_mf_holdings(conn, "TICK1", date(2026, 6, 1),
                               [("Scheme A", 1200, 1_200_000.0), ("Scheme B", 500, 500_000.0),
                                ("Scheme C", 300, 300_000.0)],
                               date(2026, 6, 15))

        r = client.get("/api/v1/big-investors/mf-holdings/movers", params={"as_of": "2026-07-01T00:00:00"})
        assert r.status_code == 200, r.text
        body = r.json()
        row = next(d for d in body["data"] if d["ticker"] == "TICK1")
        assert row["curr_scheme_count"] == 3
        assert row["prev_scheme_count"] == 2
        assert row["scheme_count_change"] == 1
        assert row["direction"] == "increasing"

    def test_new_entry_scheme_count_change_equals_curr_count(self, client, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _seed_stock_master(conn, "TICK2", "Tick Two", 3000.0)
            _seed_mf_holdings(conn, "TICK2", date(2026, 5, 1), [], date(2026, 5, 15))
            _seed_mf_holdings(conn, "TICK2", date(2026, 6, 1),
                               [("Scheme X", 400, 400_000.0)], date(2026, 6, 15))

        r = client.get("/api/v1/big-investors/mf-holdings/movers", params={"as_of": "2026-07-01T00:00:00"})
        assert r.status_code == 200, r.text
        body = r.json()
        row = next(d for d in body["data"] if d["ticker"] == "TICK2")
        assert row["direction"] == "new_entry"
        assert row["curr_scheme_count"] == 1
        assert row["prev_scheme_count"] == 0
        assert row["scheme_count_change"] == 1


# ===== _position_row_to_dict / holding_pct_of_company estimate (BI5 context) =====

class TestPositionRowToDict:
    def test_holding_pct_uses_market_cap_price_back_derivation(self):
        row = (
            "FAM1", "TICK1", date(2026, 1, 5), "BULK", "BUY", 1000, 100.0, "NSE",
            1000, True, False,
        )
        # _position_row_to_dict expects the trailing (family_display_name,
        # company_name, market_cap_cr, cmp, cmp_date) tuple appended to row;
        # build it explicitly per the function's own unpacking contract.
        full_row = row + ("Family One", "Tick One Ltd", 1000.0, 200.0, date(2026, 7, 1))
        d = bi_router._position_row_to_dict(full_row, combined_qty=1000, wac=100.0, trendlyne_prior_holder=False)
        # shares_outstanding_est = market_cap_cr * 1e7 / cmp = 1000 * 1e7 / 200 = 5e7
        # holding_pct = combined_qty / shares_outstanding_est * 100
        assert d["holding_pct_of_company"] == pytest.approx(1000 / 5e7 * 100.0)
        assert d["cap_band"] == "micro"

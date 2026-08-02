"""tests/unit/test_export_trade_book.py — smoke coverage for the
2026-08-01 trade_cagr/return_zscore/entry_signal_zscore/HTML additions to
backtest/export_trade_book.py. No real DuckDB/backtest run required — a
stub connection stands in for backtest_feature_log."""

import csv

from backtest.export_trade_book import export_trade_book


class _StubConn:
    """Mimics conn.execute(...).fetchone() for backtest_feature_log —
    returns a real feature_vector_json for TCS's buy date, None for
    everything else (no exit-day log entry, matches real "forced_close"
    behavior)."""

    def execute(self, query, params):
        ticker, as_of_date = params[1], params[2]
        if ticker == "TCS" and as_of_date == "2024-01-01":
            return _StubResult((
                '{"template_name": "A1", "matched": true, "score": 82.5, '
                '"matched_conditions": 8, "total_conditions": 10}',
                "buy",
            ))
        return _StubResult(None)


class _StubResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


def _write_trade_log_csv(path, rows):
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ticker", "qty", "buy_date", "buy_price", "sale_date", "sale_price", "stock_rank", "pnl_inr", "pnl_pct", "exit_reason"])
        for r in rows:
            writer.writerow(r)


def test_export_trade_book_writes_new_columns_and_html(tmp_path):
    trade_log = tmp_path / "trade_log_run1.csv"
    _write_trade_log_csv(trade_log, [
        ["TCS", "10", "2024-01-01", "100.0", "2024-02-01", "110.0", "5", "100.0", "0.10", "signal"],
        ["INFY", "10", "2024-01-01", "200.0", "2024-02-15", "180.0", "20", "-200.0", "-0.10", "signal"],
        ["WIPRO", "10", "2024-01-01", "50.0", "2024-03-01", "80.0", "50", "300.0", "0.60", "signal"],
    ])

    out_csv = tmp_path / "trade_book_run1.csv"
    result = export_trade_book("run1", trade_log, out_path=out_csv, conn=_StubConn(), write_html=True)

    assert result == out_csv
    rows = list(csv.DictReader(open(out_csv)))
    assert len(rows) == 3

    tcs = next(r for r in rows if r["ticker"] == "TCS")
    assert tcs["entry_reason"] == "buy"
    assert float(tcs["entry_signal_score"]) == 82.5
    assert tcs["trade_cagr"] != ""

    # return_zscore/entry_signal_zscore populated for at least the row with
    # the most extreme value once >=3 trades exist in the population.
    wipro = next(r for r in rows if r["ticker"] == "WIPRO")
    assert wipro["return_zscore"] != ""

    from backtest.export_trade_book import HTML_OUT_DIR
    html_path = HTML_OUT_DIR / "trade_book_run1.html"
    assert html_path.exists()
    html_content = html_path.read_text()
    assert "TCS" in html_content
    assert "WIPRO" in html_content
    html_path.unlink()


def test_export_trade_book_no_html_by_default(tmp_path):
    trade_log = tmp_path / "trade_log_run2.csv"
    _write_trade_log_csv(trade_log, [["TCS", "10", "2024-01-01", "100.0", "2024-02-01", "110.0", "5", "100.0", "0.10", "signal"]])
    out_csv = tmp_path / "trade_book_run2.csv"
    export_trade_book("run2", trade_log, out_path=out_csv, conn=_StubConn())

    from backtest.export_trade_book import HTML_OUT_DIR
    assert not (HTML_OUT_DIR / "trade_book_run2.html").exists()

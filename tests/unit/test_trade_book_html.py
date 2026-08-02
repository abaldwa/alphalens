"""tests/unit/test_trade_book_html.py — unit coverage for
backtest/trade_book_html.py, no real backtest run required."""

from backtest.trade_book_html import render_trade_book_html, write_html_index


def test_render_trade_book_html_writes_expected_content(tmp_path):
    rows = [
        {"ticker": "TCS", "buy_price": 100.0, "sell_price": 110.0, "pnl_pct": 0.10},
        {"ticker": "INFY", "buy_price": 200.0, "sell_price": 180.0, "pnl_pct": -0.10},
    ]
    out = tmp_path / "trade_book_A1.html"
    result = render_trade_book_html(rows, ["ticker", "buy_price", "sell_price", "pnl_pct"], "A1 baseline", out)

    assert result == out
    assert out.exists()
    content = out.read_text()
    assert "TCS" in content
    assert "INFY" in content
    assert "<title>A1 baseline</title>" in content
    assert "2 rows" in content


def test_render_trade_book_html_handles_none_values(tmp_path):
    rows = [{"ticker": "X", "sell_price": None}]
    out = tmp_path / "t.html"
    render_trade_book_html(rows, ["ticker", "sell_price"], "t", out)
    assert "&mdash;" in out.read_text()


def test_render_trade_book_html_creates_parent_dirs(tmp_path):
    out = tmp_path / "nested" / "dir" / "t.html"
    render_trade_book_html([], ["ticker"], "empty", out)
    assert out.exists()


def test_write_html_index(tmp_path):
    out = tmp_path / "index.html"
    write_html_index(
        [{"label": "A1 baseline", "href": "trade_book_A1.html"}, {"label": "A1 trailing", "href": "trade_book_A1_trailing.html"}],
        out, title="Technical Trade Books",
    )
    content = out.read_text()
    assert "Technical Trade Books" in content
    assert "trade_book_A1.html" in content
    assert "A1 trailing" in content

"""
tests/unit/test_nse_xbrl_financials.py

Unit tests for ingestion/scrapers/nse_xbrl_financials.py — mocked HTTP only.
"""

import pytest

from ingestion.scrapers import nse_xbrl_financials as nxf

_FAKE_INDAS_HTML = """
<html><body>
<h3>General information about company</h3>
<table>
<tr><th>Nature of report standalone or consolidated</th><td>Consolidated</td></tr>
</table>
<h3>Financial Results Ind-AS</h3>
<table>
<tr><th>Revenue from operations</th><td>1,00,000.00</td></tr>
<tr><th></th><th>Paid-up equity share capital</th><td>13,53,200.00</td><td>13,53,200.00</td></tr>
<tr><th></th><th>Face value of equity share capital</th><td>10</td><td>10</td></tr>
</table>
<h3>Statement of Asset and Liabilities</h3>
<table>
<tr><th>Date of end of reporting period</th><td>30-09-2025</td></tr>
<tr><th>Goodwill</th><td>1,000.00</td></tr>
<tr><th>Capital work-in-progress</th><td>500.00</td></tr>
<tr><th>Inventories</th><td>2,000.00</td></tr>
<tr style="background-color:lightgray;"><td></td><th class="txtAlign"><b>Total current assets</b></th><td class="subtitle_bg"><b>10,000.00</b></td></tr>
<tr><th>Total current liabilities</th><td>4,000.00</td></tr>
<tr><th>Total liabilities</th><td>6,000.00</td></tr>
<tr><th>Total equity and liabilites</th><td>20,000.00</td></tr>
</table>
<h3>Format for Reporting Segment wise Revenue</h3>
<table></table>
<h3>Details of Impact of Audit Qualification</h3>
<table>
<tr><th>Declaration of unmodified opinion or statement on impact of audit qualification</th><td>Declaration of unmodified opinion</td></tr>
</table>
</body></html>
"""


_FAKE_RESULTS_ONLY_HTML = """
<html><body>
<h3>General information about company</h3>
<table>
<tr><th>Nature of report standalone or consolidated</th><td>Standalone</td></tr>
</table>
<h3>Financial Results Ind-AS</h3>
<table>
<tr><th>A</th><th>Date of start of reporting period</th><td>01-10-2025</td><td>01-04-2025</td></tr>
<tr><th>B</th><th>Date of end of reporting period</th><td>31-12-2025</td><td>31-12-2025</td></tr>
<tr><th></th><th>Paid-up equity share capital</th><td>58,00,200.00</td><td>58,00,200.00</td></tr>
<tr><th></th><th>Face value of equity share capital</th><td>1</td><td>1</td></tr>
</table>
<h3>Format for Reporting Segment wise Revenue</h3>
<table></table>
</body></html>
"""


class _FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        import json

        return json.loads(self.text)


class _FakeSession:
    def __init__(self, text):
        self._text = text

    def get(self, url, params=None, timeout=None):
        return _FakeResponse(self._text)


class TestParseAmount:
    def test_indian_grouping_and_lakh_to_crore(self):
        # 7,51,08,700.00 Lakh * 0.01 = 751087.0 Cr — matches real RELIANCE
        # property_plant_equipment, live-verified against the actual filing.
        assert nxf._parse_amount("7,51,08,700.00") == pytest.approx(751087.0)

    def test_negative_parenthesized(self):
        assert nxf._parse_amount("(10,26,700.00)") == pytest.approx(-10267.0)

    def test_blank_and_dash(self):
        assert nxf._parse_amount("") is None
        assert nxf._parse_amount("-") is None
        assert nxf._parse_amount(None) is None


class TestDownloadIndasFiling:
    def test_parses_real_shaped_sections(self, monkeypatch):
        monkeypatch.setattr(nxf, "_nse_session", lambda: _FakeSession(_FAKE_INDAS_HTML))
        result = nxf.download_indas_filing("https://fake/url.html")
        assert result["quarter_end_date"] == "30-09-2025"
        assert result["consolidated"] is True
        assert result["goodwill"] == pytest.approx(10.0)  # 1,000 Lakh -> 10 Cr
        assert result["cwip"] == pytest.approx(5.0)
        assert result["inventories"] == pytest.approx(20.0)
        assert result["current_assets"] == pytest.approx(100.0)  # styled <tr> row must still parse
        assert result["current_liabilities"] == pytest.approx(40.0)
        assert result["total_liabilities"] == pytest.approx(60.0)
        assert result["total_assets"] == pytest.approx(200.0)
        assert result["audit_qualified_flag"] is False
        # 1,353,200,000 (paid-up capital, raw INR) / 10 (face value) = 135,320,000,000
        assert result["shares_outstanding"] == 13532000000

    def test_results_only_filing_with_no_balance_sheet_still_parses(self, monkeypatch):
        """Real gap found in full-universe verification: SEBI LODR only mandates a full
        balance sheet at half-year/year-end — Q1/Q3 'results only' filings genuinely have
        no 'Statement of Asset and Liabilities' section at all. Must not be dropped."""
        monkeypatch.setattr(nxf, "_nse_session", lambda: _FakeSession(_FAKE_RESULTS_ONLY_HTML))
        result = nxf.download_indas_filing("https://fake/url.html")
        assert result["quarter_end_date"] == "31-12-2025"
        assert result["consolidated"] is False
        # Lakh-scaled interpretation (580,020,000,000 shares) exceeds the
        # plausibility bound, so the raw-rupee interpretation correctly wins.
        assert result["shares_outstanding"] == 5800200
        assert "goodwill" not in result  # genuinely absent, not fabricated as 0/None

    def test_raw_rupee_paid_up_capital_not_double_scaled(self, monkeypatch):
        """Real gap (ACC's real filing): some filings report paid-up capital as a plain raw-
        rupee integer ('1879900000', no comma/decimal), not Lakh-scaled like every other
        figure in this section. Applying the Lakh->Crore conversion inflated shares by 1e5x."""
        html = """
        <html><body>
        <h3>General information about company</h3>
        <table><tr><th>Nature of report standalone or consolidated</th><td>Standalone</td></tr></table>
        <h3>Financial Results Ind-AS</h3>
        <table>
        <tr><th>B</th><th>Date of end of reporting period</th><td>30-Jun-2025</td><td></td></tr>
        <tr><th></th><th>Paid-up equity share capital</th><td>1879900000</td><td></td></tr>
        <tr><th></th><th>Face value of equity share capital</th><td>10</td><td></td></tr>
        </table>
        <h3>Format for Reporting Segment wise Revenue</h3>
        <table></table>
        </body></html>
        """
        monkeypatch.setattr(nxf, "_nse_session", lambda: _FakeSession(html))
        result = nxf.download_indas_filing("https://fake/url.html")
        assert result["shares_outstanding"] == 187990000  # real ACC-like share count, not 1.88e13

    def test_raw_rupee_paid_up_capital_with_commas_not_lakh_scaled(self, monkeypatch):
        """Real gap (AARON's real filing): comma presence alone does NOT mean Lakh-scaled —
        some filings report a comma-grouped value ('20,94,64,780.00') that is ALSO already
        raw rupees, not Lakhs. Lakh-scaling it would give 2.09 trillion "shares" for a real
        small-cap with ~2.1 crore shares. Resolved via plausibility, not formatting."""
        html = """
        <html><body>
        <h3>General information about company</h3>
        <table><tr><th>Nature of report standalone or consolidated</th><td>Standalone</td></tr></table>
        <h3>Financial Results Ind-AS</h3>
        <table>
        <tr><th>B</th><th>Date of end of reporting period</th><td>31-Mar-2026</td><td></td></tr>
        <tr><th></th><th>Paid-up equity share capital</th><td>20,94,64,780.00</td><td>20,94,64,780.00</td></tr>
        <tr><th></th><th>Face value of equity share capital</th><td>10</td><td>10</td></tr>
        </table>
        <h3>Format for Reporting Segment wise Revenue</h3>
        <table></table>
        </body></html>
        """
        monkeypatch.setattr(nxf, "_nse_session", lambda: _FakeSession(html))
        result = nxf.download_indas_filing("https://fake/url.html")
        assert result["shares_outstanding"] == 20946478  # real AARON-like share count, not 2.09e12


class TestParseSharesOutstanding:
    def test_derives_from_paid_up_capital_and_face_value(self):
        assert nxf._parse_shares_outstanding(_FAKE_INDAS_HTML) == 13532000000

    def test_returns_none_when_section_missing(self):
        assert nxf._parse_shares_outstanding("<html><body></body></html>") is None

    def test_raises_after_retries_on_connection_failure(self, monkeypatch):
        def _raise_session():
            raise nxf.requests.RequestException("unreachable")

        monkeypatch.setattr(nxf, "_nse_session", _raise_session)
        with pytest.raises(ConnectionError):
            nxf.download_indas_filing("https://fake/url.html")


class TestListIntegratedFilings:
    def test_filters_to_indas_rows_only(self, monkeypatch):
        payload = (
            '{"data": ['
            '{"ixbrl": "https://x/INTEGRATED_FILING_INDAS_1.html"},'
            '{"ixbrl": "https://x/INTEGRATED_FILING_GOVERNANCE_2.html"}'
            "]}"
        )
        monkeypatch.setattr(nxf, "_nse_session", lambda: _FakeSession(payload))
        rows = nxf.list_integrated_filings("FAKE")
        assert len(rows) == 1
        assert "INDAS" in rows[0]["ixbrl"]


class TestFetchIndasHtmlCaching:
    def test_writes_to_cache_on_first_fetch(self, monkeypatch, tmp_path):
        monkeypatch.setattr(nxf, "_nse_session", lambda: _FakeSession(_FAKE_INDAS_HTML))
        html = nxf.fetch_indas_html("https://fake/url.html", seq_id="123", cache_dir=tmp_path)
        assert html == _FAKE_INDAS_HTML
        assert (tmp_path / "123.html").read_text() == _FAKE_INDAS_HTML

    def test_reads_from_cache_without_network_on_second_fetch(self, monkeypatch, tmp_path):
        (tmp_path / "123.html").write_text("cached content")

        def _raise_session():
            raise AssertionError("must not hit the network when cache exists")

        monkeypatch.setattr(nxf, "_nse_session", _raise_session)
        html = nxf.fetch_indas_html("https://fake/url.html", seq_id="123", cache_dir=tmp_path)
        assert html == "cached content"

    def test_no_cache_dir_always_fetches_live(self, monkeypatch):
        calls = {"n": 0}

        def _session():
            calls["n"] += 1
            return _FakeSession(_FAKE_INDAS_HTML)

        monkeypatch.setattr(nxf, "_nse_session", _session)
        nxf.fetch_indas_html("https://fake/url.html", seq_id="123", cache_dir=None)
        nxf.fetch_indas_html("https://fake/url.html", seq_id="123", cache_dir=None)
        assert calls["n"] == 2


class TestIngestionStateTracking:
    def test_ensure_and_mark_and_get_roundtrip(self, tmp_path):
        import sqlite3

        conn = sqlite3.connect(tmp_path / "state.db")
        nxf.ensure_ingested_filings_table(conn)
        assert nxf.get_ingested_seq_ids(conn) == set()

        nxf.mark_filings_ingested(
            conn,
            [{"seq_id": "1", "ticker": "FAKE", "fiscal_year": 2026, "quarter": 4}],
        )
        assert nxf.get_ingested_seq_ids(conn) == {"1"}

    def test_replace_lets_a_later_more_complete_record_win(self, tmp_path):
        import sqlite3

        conn = sqlite3.connect(tmp_path / "state.db")
        nxf.ensure_ingested_filings_table(conn)
        nxf.mark_filings_ingested(conn, [{"seq_id": "1", "ticker": "FAKE", "fiscal_year": None, "quarter": None}])
        nxf.mark_filings_ingested(conn, [{"seq_id": "1", "ticker": "FAKE", "fiscal_year": 2026, "quarter": 4}])
        row = conn.execute("SELECT fiscal_year, quarter FROM nse_xbrl_ingested_filings WHERE seq_id='1'").fetchone()
        assert row == (2026, 4)

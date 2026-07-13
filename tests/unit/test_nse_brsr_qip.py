"""
tests/unit/test_nse_brsr_qip.py

CA6 (2026-07-10): unit tests for ingestion/scrapers/nse_brsr_qip.py.
Fixtures below are the REAL JSON shapes captured live 2026-07-10 against
IDFCFIRSTB/ZOMATO (QIP) and RELIANCE (BRSR) — see the module docstring for
the verification detail. Mocked at the requests.Session level, no real
network calls in this test file.
"""

from unittest.mock import MagicMock, patch

from ingestion.scrapers.nse_brsr_qip import download_brsr_filings, download_qip_issues

_REAL_QIP_ROW = {
    "appId": "43078", "boardResolutionDate": "09-OCT-2023", "companyName": "IDFC FIRST Bank Limited",
    "corporateIdentityNumber": "L65110TN2014PLC097792", "dateOfListing": "10-OCT-2023",
    "dateOfSubmission": "08-OCT-2023", "dateOfTradingApproval": "10-OCT-2023",
    "distPerShrsAvailed": "4.7", "dtOfAllotmentOfShares": "06-OCT-2023", "dtOfBIDClosing": "06-OCT-2023",
    "dtOfBIDOpening": "03-OCT-2023", "finalAmountOfIssueSize": "30000000000", "isin": "INE092T01019",
    "issPricePerUnit": "90.25", "issue_type": "QIP", "minIssPricePerUnit": "94.95",
    "noOfAllottees": "69", "noOfEquitySharesListed": "332409972", "noOfSharesAllotted": "332409972",
    "nsesymbol": "IDFCFIRSTB", "relavantDt": "03-OCT-2023", "revisedFlag": None, "stage": "Listing Stage",
    "xbrlFileSize": None, "xmlFileName": "https://nsearchives.nseindia.com/corporate/xbrl/QIP_LS_942812.xml",
}

_REAL_BRSR_ROW = {
    "attFileSize": "9.16 MB", "attachmentFile": "https://nsearchives.nseindia.com/corporate/BRSR_500325.pdf",
    "companyName": "Reliance Industries Limited", "fyFrom": 2025, "fyTo": 2026, "revisionDate": "-",
    "submissionDate": "06-Jun-2026", "symbol": "RELIANCE",
    "xbrlFile": "https://nsearchives.nseindia.com/corporate/xbrl/BRSR_500325.xml", "xbrlFileSize": "1.47 MB",
}


def _mock_session(json_data):
    session = MagicMock()
    session.get.return_value = MagicMock(status_code=200, json=lambda: {"data": json_data})
    session.get.return_value.raise_for_status = lambda: None
    return session


class TestDownloadQipIssues:
    def test_parses_real_qip_row_shape(self):
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([_REAL_QIP_ROW])):
            rows = download_qip_issues("IDFCFIRSTB")
        assert len(rows) == 1
        row = rows[0]
        assert row["ticker"] == "IDFCFIRSTB"
        assert row["app_id"] == "43078"
        assert row["issue_price"] == 90.25
        assert row["no_of_allottees"] == 69
        assert row["no_of_shares_allotted"] == 332409972
        assert row["dilution_pct"] == 332409972 / 332409972  # == 1.0 for this real row (first-issue edge case)

    def test_non_qip_issue_type_is_filtered_out(self):
        other = dict(_REAL_QIP_ROW, issue_type="RIGHTS")
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([other])):
            rows = download_qip_issues("IDFCFIRSTB")
        assert rows == []

    def test_empty_data_returns_empty_list(self):
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([])):
            rows = download_qip_issues("RELIANCE")
        assert rows == []

    def test_missing_share_counts_leaves_dilution_pct_none(self):
        row = dict(_REAL_QIP_ROW, noOfSharesAllotted=None)
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([row])):
            rows = download_qip_issues("IDFCFIRSTB")
        assert rows[0]["dilution_pct"] is None
        assert rows[0]["no_of_shares_allotted"] is None


class TestDownloadBrsrFilings:
    def test_parses_real_brsr_row_shape(self):
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([_REAL_BRSR_ROW])):
            rows = download_brsr_filings("RELIANCE")
        assert len(rows) == 1
        row = rows[0]
        assert row["ticker"] == "RELIANCE"
        assert row["fy_from"] == 2025
        assert row["fy_to"] == 2026
        assert row["xbrl_file_url"] == "https://nsearchives.nseindia.com/corporate/xbrl/BRSR_500325.xml"

    def test_row_missing_fy_to_is_dropped(self):
        row = dict(_REAL_BRSR_ROW, fyTo=None)
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([row])):
            rows = download_brsr_filings("RELIANCE")
        assert rows == []

    def test_empty_data_returns_empty_list(self):
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=_mock_session([])):
            rows = download_brsr_filings("SOMECO")
        assert rows == []


class TestConnectionErrorHandling:
    def test_get_json_raises_connection_error_after_retries(self):
        import requests as requests_module

        session = MagicMock()
        session.get.side_effect = requests_module.RequestException("boom")
        with patch("ingestion.scrapers.nse_brsr_qip._nse_session", return_value=session):
            try:
                download_qip_issues("BADTICKER")
                assert False, "expected ConnectionError"
            except ConnectionError:
                pass

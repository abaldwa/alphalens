"""
tests/unit/test_corporate_actions_scraper.py

Regression test for a bug found 2026-08-04: download_corporate_actions()
crashed with "unsupported operand type(s) for -: 'str' and
'datetime.timedelta'" whenever a record had a record_date, because
_parse_nse_date() returns an ISO-format string (not a date/datetime
object), and announcement_date was computed as
`record_date - timedelta(days=CORP_ACTION_NOTICE_DAYS)` directly on that
string. This only manifested on days whose NSE response actually included
a populated recDate field (e.g. 2026-07-31, 2026-08-03 backfill/catch-up
dates), which is why it wasn't caught earlier.
"""

from unittest.mock import patch

from ingestion.scrapers.corporate_actions import download_corporate_actions


def _record(symbol="RELIANCE", series="EQ", ex_date="31-JUL-2026",
            rec_date="07-AUG-2026", purpose="BONUS 1:1"):
    return {
        "symbol": symbol,
        "series": series,
        "exDate": ex_date,
        "recDate": rec_date,
        "purpose": purpose,
    }


class TestDownloadCorporateActionsDateArithmetic:
    def test_record_with_record_date_does_not_raise_typeerror(self):
        """record_date present -> announcement_date arithmetic must not
        blow up on a str operand (the original bug)."""
        with patch(
            "ingestion.scrapers.corporate_actions._fetch_corporate_actions_json",
            return_value=[_record()],
        ), patch("ingestion.scrapers.corporate_actions._save_raw"):
            df = download_corporate_actions("2026-07-31", filter_by_date=False)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["record_date"] == "2026-08-07"
        # CORP_ACTION_NOTICE_DAYS = 7 -> 2026-08-07 - 7 days = 2026-07-31
        assert row["announcement_date"] == "2026-07-31"
        assert isinstance(row["announcement_date"], str)

    def test_record_without_record_date_leaves_announcement_date_none(self):
        with patch(
            "ingestion.scrapers.corporate_actions._fetch_corporate_actions_json",
            return_value=[_record(rec_date="-")],
        ), patch("ingestion.scrapers.corporate_actions._save_raw"):
            df = download_corporate_actions("2026-07-31", filter_by_date=False)

        assert len(df) == 1
        assert df.iloc[0]["record_date"] is None
        assert df.iloc[0]["announcement_date"] is None

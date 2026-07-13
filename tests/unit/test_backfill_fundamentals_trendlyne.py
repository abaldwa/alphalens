"""
tests/unit/test_backfill_fundamentals_trendlyne.py

Covers scripts/backfill_fundamentals_trendlyne.py's _fetch_ticker_data
status-code classification, fixed 2026-07-13 after the two live backfill
runs on record (logs/trendlyne_backfill.log 2026-06-25,
logs/trendlyne_backfill_full2644_20260630.log 2026-06-30) both collapsed
to near-0% success mid-run.

Root cause (confirmed via a live re-check on 2026-07-13, see
FeatureBacklog.md): those 405s were Trendlyne WAF/rate-limit responses,
NOT a ticker-matching/URL bug — the same tickers (including large caps
like ADANIPORTS) resolve fine today with identical logic. The bug was
that the OLD code treated 405 exactly like a genuine 404 "not on
Trendlyne" and applied the fast 0.3x notfound retry delay, which fed the
block instead of backing off from it. This module now returns a `reason`
string ("ok"/"404"/"405"/"error") so callers can apply a full-length
backoff (and a circuit-breaker re-login) specifically for 405, instead of
skipping fast the way a genuine miss should.
"""

from unittest.mock import MagicMock


from scripts.backfill_fundamentals_trendlyne import _fetch_ticker_data


def _resp(status_code, text="", url="https://trendlyne.com/equity/X/x/"):
    r = MagicMock()
    r.status_code = status_code
    r.text = text
    r.url = url
    return r


class TestFetchTickerDataClassification:
    def test_200_with_tablesurl_and_ok_json_returns_ok(self):
        session = MagicMock()
        company_page = _resp(200, text='data-tablesurl=https://trendlyne.com/tables/x.json')
        table_resp = MagicMock()
        table_resp.status_code = 200
        table_resp.text = '{"head": {"status": "0"}, "body": {"quarterlyOrder": []}}'
        table_resp.json.return_value = {"head": {"status": "0"}, "body": {"quarterlyOrder": []}}
        session.get.side_effect = [company_page, table_resp]

        body, reason = _fetch_ticker_data(session, "TCS")

        assert reason == "ok"
        assert body == {"quarterlyOrder": []}

    def test_404_is_classified_as_genuine_miss(self):
        session = MagicMock()
        session.get.return_value = _resp(404)

        body, reason = _fetch_ticker_data(session, "NOTAREALCOMPANY")

        assert body is None
        assert reason == "404"

    def test_405_with_failing_dash_slug_fallback_is_classified_as_405_not_404(self):
        """
        [Fix 2026-07-13] This is the exact case the old code got wrong:
        a 405 (WAF/rate-limit signal) must NOT be silently folded into
        the same bucket as a genuine 404 — the caller needs to tell them
        apart to avoid feeding a block with the fast notfound retry.
        """
        session = MagicMock()
        session.get.side_effect = [_resp(405), _resp(405)]  # primary, then dash-slug fallback

        body, reason = _fetch_ticker_data(session, "ADANIPORTS")

        assert body is None
        assert reason == "405"

    def test_405_that_succeeds_via_dash_slug_fallback_still_returns_ok(self):
        session = MagicMock()
        company_page = _resp(405)
        alt_page = _resp(200, text='data-tablesurl=https://trendlyne.com/tables/x.json')
        table_resp = MagicMock()
        table_resp.status_code = 200
        table_resp.text = '{"head": {"status": "0"}, "body": {"a": 1}}'
        table_resp.json.return_value = {"head": {"status": "0"}, "body": {"a": 1}}
        session.get.side_effect = [company_page, alt_page, table_resp]

        body, reason = _fetch_ticker_data(session, "M&M")

        assert reason == "ok"
        assert body == {"a": 1}

    def test_403_is_classified_in_the_same_bucket_as_405(self):
        session = MagicMock()
        session.get.return_value = _resp(403)

        body, reason = _fetch_ticker_data(session, "SBIN")

        assert body is None
        assert reason == "405"

    def test_network_error_is_classified_as_error(self):
        import requests

        session = MagicMock()
        session.get.side_effect = requests.RequestException("boom")

        body, reason = _fetch_ticker_data(session, "INFY")

        assert body is None
        assert reason == "error"

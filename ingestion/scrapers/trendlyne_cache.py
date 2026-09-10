"""
ingestion/scrapers/trendlyne_cache.py

[2026-09-06] Two-phase Trendlyne fundamentals backfill:
  Phase 1 (SCRAPE): Fetch from Trendlyne in small batches, cache locally.
                    No DuckDB lock. Graceful backoff on WAF rate-limiting.
  Phase 2 (PERSIST): Read cache, validate, batch-write to DuckDB.
                     Separate job, runs when cache has data.

This eliminates:
- Lock contention (scraping never touches DB)
- WAF rate-limiting blocking other jobs (scraping resilient to timeouts)
- Cascading failures (partial cache survives, can be persisted later)
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# Cache directory for raw Trendlyne responses
TRENDLYNE_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "datastore" / "cache" / "trendlyne"
TRENDLYNE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Rate-limit backoff parameters
INITIAL_BACKOFF_SEC = 60
MAX_BACKOFF_SEC = 600
WAF_ERROR_THRESHOLD = 5  # consecutive 405/403s before backing off

# [2026-09-10 fix] The persistent 403 this scraper was hitting was NOT a
# WAF/IP block — it was hitting the wrong endpoints entirely. This class
# originally POSTed JSON to a nonexistent /api/user/login/ with no
# browser-like headers, then GET a /v2/stock/{ticker}/fundamental_results/
# URL that doesn't exist either. scripts/backfill_fundamentals_trendlyne.py
# (in production, working) does it correctly: GET the real Django login
# page (/accounts/login/) for a CSRF token, POST form-encoded credentials
# with that token plus a full browser header set, then for each ticker GET
# the company page (/equity/{ticker}/{slug}/) and follow its embedded
# `data-tablesurl` to the actual JSON endpoint. Ported that proven flow
# here rather than inventing a second, independently-drifting copy of it.
BASE_URL = "https://trendlyne.com"
LOGIN_URL = f"{BASE_URL}/accounts/login/"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class TrendlyneScraperCache:
    """
    Fetch Trendlyne fundamentals in small batches, cache locally.
    No DuckDB locks. Graceful backoff on rate-limiting.
    """

    def __init__(self, username: str, password: str, batch_size: int = 50, backoff_sec: int = 60) -> None:
        self.username = username
        self.password = password
        self.batch_size = batch_size
        self.backoff_sec = backoff_sec
        self.session: Optional[requests.Session] = None
        self.consecutive_errors = 0

    def login(self) -> bool:
        """Login to Trendlyne and establish session (real CSRF-based Django login)."""
        try:
            self.session = requests.Session()
            self.session.headers.update(_HEADERS)

            page = self.session.get(LOGIN_URL, timeout=30)
            csrf_m = re.search(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"', page.text)
            if not csrf_m:
                logger.warning("Trendlyne login: could not find CSRF token on login page")
                return False
            csrf = csrf_m.group(1)

            resp = self.session.post(
                LOGIN_URL,
                data={
                    "csrfmiddlewaretoken": csrf,
                    "login": self.username,
                    "password": self.password,
                    "recaptcha_token": "",
                    "recaptcha_action": "login",
                    "remember": "on",
                },
                headers={"Referer": LOGIN_URL},
                timeout=30,
            )
            if resp.status_code >= 400 or "id_password" in resp.text:
                logger.warning(f"Trendlyne login failed (status={resp.status_code})")
                return False

            logger.info(f"Trendlyne login successful → {resp.url}")
            self.consecutive_errors = 0
            return True
        except Exception as e:
            logger.warning(f"Trendlyne login error: {e}")
            return False

    def fetch_ticker(self, ticker: str, cache_only: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch fundamentals for one ticker, cache result locally.

        Args:
            ticker: plain NSE symbol, e.g. "SBIN" (no exchange suffix —
                matches fundamentals.ticker's stored format)
            cache_only: If True, return cached data without fetching

        Returns:
            Dict with fundamentals or None if not found
        """
        cache_file = TRENDLYNE_CACHE_DIR / f"{ticker}.json"

        # If cache_only, return cached data if available
        if cache_only:
            if cache_file.exists():
                try:
                    with open(cache_file) as f:
                        cached_data: Dict[str, Any] = json.load(f)
                        return cached_data
                except (json.JSONDecodeError, IOError):
                    return None
            return None

        # Try to fetch
        if not self.session:
            logger.warning("Not logged in, skipping fetch")
            return None

        try:
            # Two real requests: company page (embeds a session-specific
            # data-tablesurl), then that tablesurl for the actual JSON.
            company_url = f"{BASE_URL}/equity/{ticker}/{ticker.lower()}/"
            resp = self.session.get(company_url, timeout=30)

            if resp.status_code == 404:
                logger.debug(f"{ticker}: 404 on Trendlyne (not listed)")
                return None
            if resp.status_code in (403, 405, 410):
                self.consecutive_errors += 1
                if self.consecutive_errors >= WAF_ERROR_THRESHOLD:
                    logger.warning(
                        f"[{self.consecutive_errors}/{WAF_ERROR_THRESHOLD}] "
                        f"WAF rate-limiting detected ({resp.status_code}s). "
                        f"Backing off {self.backoff_sec}s before continuing."
                    )
                    time.sleep(self.backoff_sec)
                    self.backoff_sec = int(min(self.backoff_sec * 1.5, MAX_BACKOFF_SEC))
                    self.consecutive_errors = 0
                return None
            if resp.status_code != 200:
                logger.debug(f"{ticker}: company page HTTP {resp.status_code}, skipping")
                return None

            tablesurl_m = re.search(r'data-tablesurl=(https://[^\s>]+)', resp.text)
            if not tablesurl_m:
                logger.debug(f"{ticker}: data-tablesurl not found on company page")
                return None
            tablesurl = tablesurl_m.group(1)

            rj = self.session.get(
                tablesurl, timeout=30,
                headers={"X-Requested-With": "XMLHttpRequest", "Referer": company_url},
            )
            if rj.status_code != 200 or not rj.text.strip():
                logger.debug(f"{ticker}: tablesurl HTTP {rj.status_code}")
                return None

            try:
                fetched_data: Dict[str, Any] = rj.json()
            except ValueError:
                # [2026-09-10] Observed live: Trendlyne intermittently (not
                # tied to any one ticker — same URL that returns clean JSON
                # on one pass returns DRF's browsable-API HTML wrapper on
                # another) serves this instead of the raw JSON despite a
                # 200 status and no distinct 403/405/410. The production
                # script (backfill_fundamentals_trendlyne.py) hits the same
                # thing and just logs+skips; treating it as a WAF signal
                # here (not a hard error) lets the existing backoff/re-login
                # circuit-breaker react to a run of these the same way it
                # already does for explicit 403/405s.
                self.consecutive_errors += 1
                logger.debug(f"{ticker}: tablesurl returned non-JSON (DRF browsable-HTML quirk)")
                if self.consecutive_errors >= WAF_ERROR_THRESHOLD:
                    logger.warning(
                        f"[{self.consecutive_errors}/{WAF_ERROR_THRESHOLD}] "
                        f"Non-JSON responses detected — backing off {self.backoff_sec}s."
                    )
                    time.sleep(self.backoff_sec)
                    self.backoff_sec = int(min(self.backoff_sec * 1.5, MAX_BACKOFF_SEC))
                    self.consecutive_errors = 0
                return None

            with open(cache_file, "w") as f:
                json.dump(fetched_data, f)
            self.consecutive_errors = 0
            logger.debug(f"Cached {ticker}")
            return fetched_data
        except requests.Timeout:
            logger.debug(f"{ticker}: timeout, will retry later")
            return None
        except Exception as e:
            logger.debug(f"{ticker}: {e}")
            return None

    def fetch_batch(self, tickers: List[str], progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Fetch a batch of tickers, cache results.
        Returns dict of {ticker: fundamentals}.
        """
        results: Dict[str, Dict[str, Any]] = {}
        for i, ticker in enumerate(tickers):
            if progress_callback:
                progress_callback(i, len(tickers), results)
            data = self.fetch_ticker(ticker, cache_only=False)
            if data:
                results[ticker] = data
            # Brief delay between requests (polite scraping)
            time.sleep(0.5)
        return results

    def get_cached_tickers(self) -> List[str]:
        """List all cached tickers (without refetching)."""
        return [f.stem for f in TRENDLYNE_CACHE_DIR.glob("*.json")]

    def clear_old_cache(self, days: int = 30) -> int:
        """Delete cached files older than N days."""
        cutoff = datetime.now().timestamp() - (days * 86400)
        deleted = 0
        for cache_file in TRENDLYNE_CACHE_DIR.glob("*.json"):
            if cache_file.stat().st_mtime < cutoff:
                cache_file.unlink()
                deleted += 1
        return deleted


def main() -> None:
    """Example: Fetch 50 tickers in batch, cache results."""
    import os

    username = os.getenv("TRENDLYNE_USERNAME")
    password = os.getenv("TRENDLYNE_PASSWORD")

    if not username or not password:
        logger.error("TRENDLYNE_USERNAME/PASSWORD not in .env")
        return

    scraper = TrendlyneScraperCache(username, password, batch_size=50)
    if not scraper.login():
        logger.error("Login failed")
        return

    # Mock tickers for demo
    tickers = ["SBIN", "INFY", "TCS", "RELIANCE"]

    def progress(current: int, total: int, results: Dict[str, Any]) -> None:
        logger.info(f"Progress: {current}/{total} tickers, {len(results)} cached")

    results = scraper.fetch_batch(tickers, progress_callback=progress)
    logger.info(f"Cached {len(results)} tickers to {TRENDLYNE_CACHE_DIR}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

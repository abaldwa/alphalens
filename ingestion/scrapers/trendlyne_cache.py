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
WAF_ERROR_THRESHOLD = 5  # consecutive 405s before backing off


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
        """Login to Trendlyne and establish session."""
        try:
            self.session = requests.Session()
            login_url = "https://trendlyne.com/markets-today-loggedin/"
            self.session.post(
                "https://trendlyne.com/api/user/login/",
                json={"email": self.username, "password": self.password},
                timeout=10,
            )
            # Verify login by making a small request
            assert self.session is not None
            resp = self.session.get(login_url, timeout=10)
            if resp.status_code == 200:
                logger.info(f"Trendlyne login successful → {login_url}")
                self.consecutive_errors = 0
                return True
            else:
                logger.warning(f"Trendlyne login failed (status={resp.status_code})")
                return False
        except Exception as e:
            logger.warning(f"Trendlyne login error: {e}")
            return False

    def fetch_ticker(self, ticker: str, cache_only: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch fundamentals for one ticker, cache result locally.

        Args:
            ticker: e.g., "SBIN.NS"
            cache_only: If True, return cached data without fetching

        Returns:
            Dict with fundamentals or None if not found
        """
        cache_file = TRENDLYNE_CACHE_DIR / f"{ticker.replace('.', '_')}.json"

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
            url = f"https://trendlyne.com/v2/stock/{ticker}/fundamental_results/"
            assert self.session is not None
            resp = self.session.get(url, timeout=15)

            if resp.status_code == 200:
                fetched_data: Dict[str, Any] = resp.json()
                # Cache successful response
                with open(cache_file, "w") as f:
                    json.dump(fetched_data, f)
                self.consecutive_errors = 0
                logger.debug(f"Cached {ticker}")
                return fetched_data
            elif resp.status_code == 405:
                self.consecutive_errors += 1
                if self.consecutive_errors >= WAF_ERROR_THRESHOLD:
                    logger.warning(
                        f"[{self.consecutive_errors}/{WAF_ERROR_THRESHOLD}] "
                        f"WAF rate-limiting detected (405s). "
                        f"Backing off {self.backoff_sec}s before continuing."
                    )
                    time.sleep(self.backoff_sec)
                    self.backoff_sec = int(min(self.backoff_sec * 1.5, MAX_BACKOFF_SEC))
                    self.consecutive_errors = 0
                return None
            else:
                logger.debug(f"{ticker}: HTTP {resp.status_code}, skipping")
                return None
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
        return [f.stem.replace("_", ".") for f in TRENDLYNE_CACHE_DIR.glob("*.json")]

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
    tickers = ["SBIN.NS", "INFY.NS", "TCS.NS", "RELIANCE.NS"]

    def progress(current: int, total: int, results: Dict[str, Any]) -> None:
        logger.info(f"Progress: {current}/{total} tickers, {len(results)} cached")

    results = scraper.fetch_batch(tickers, progress_callback=progress)
    logger.info(f"Cached {len(results)} tickers to {TRENDLYNE_CACHE_DIR}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

"""
alphalens/core/ingestion/fundamental.py

Scrapes fundamental data from Screener.in for all Nifty200 stocks.

Scraped fields:
  Valuation: P/E, P/B, P/S, EV/EBITDA, Market Cap
  Earnings:  EPS, EPS growth YoY, EPS growth 3yr
  Quality:   ROE, ROCE, Net profit margin
  Growth:    Revenue, Revenue growth
  Balance:   Debt/Equity, Current ratio
  Holdings:  Promoter %, FII %, DII %

Rate limit: 2 seconds between requests (configurable).
Full Nifty200 scrape: ~7 minutes.
Runs every Monday EOD via APScheduler.

Usage:
    scraper = FundamentalScraper()
    scraper.scrape_all()              # All 200 stocks
    scraper.scrape_symbol("RELIANCE") # Single stock
"""

import re
import time
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from loguru import logger

from alphalens.core.database import get_duck
from alphalens.core.ingestion.universe import get_all_symbols
from config.settings import settings

SCREENER_BASE     = "https://www.screener.in/company"
RATE_LIMIT_S      = 2.0
MAX_RETRIES       = 3
SESSION_HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}


class FundamentalScraper:

    def __init__(self):
        self.con     = get_duck()
        self.session = requests.Session()
        self.session.headers.update(SESSION_HEADERS)

    # ── Public API ─────────────────────────────────────────────────────────

    def scrape_all(self, symbols: Optional[list] = None) -> dict:
        """
        Scrape fundamentals for all (or given) symbols.
        Returns stats: {ok, failed, duration_minutes}
        """
        if symbols is None:
            symbols = get_all_symbols()

        logger.info(f"Starting fundamental scrape: {len(symbols)} stocks "
                    f"(~{len(symbols) * RATE_LIMIT_S / 60:.0f} min)")
        ok, failed = 0, []
        t_start = time.time()

        for idx, symbol in enumerate(symbols, 1):
            try:
                result = self.scrape_symbol(symbol)
                if result:
                    ok += 1
                    logger.debug(f"[{idx}/{len(symbols)}] {symbol}: P/E={result.get('pe_ratio')}")
                else:
                    failed.append(symbol)
            except Exception as e:
                logger.warning(f"[{idx}/{len(symbols)}] {symbol}: scrape failed — {e}")
                failed.append(symbol)

            time.sleep(RATE_LIMIT_S)

        duration = (time.time() - t_start) / 60
        logger.info(f"Fundamental scrape complete: {ok} ok, {len(failed)} failed, "
                    f"{duration:.1f} min")
        return {"ok": ok, "failed": failed, "duration_minutes": round(duration, 1)}

    def scrape_symbol(self, symbol: str) -> Optional[dict]:
        """
        Scrape Screener.in for one symbol.
        Returns dict of fundamentals or None on failure.
        """
        # Try consolidated first, fall back to standalone
        for path in [f"{SCREENER_BASE}/{symbol}/consolidated/",
                     f"{SCREENER_BASE}/{symbol}/"]:
            html = self._fetch_html(path)
            if html:
                data = self._parse(html, symbol)
                if data:
                    self._store(symbol, data)
                    return data

        logger.debug(f"{symbol}: not found on Screener.in")
        return None

    # ── HTML Parsing ───────────────────────────────────────────────────────

    def _parse(self, html: str, symbol: str) -> Optional[dict]:
        """Parse Screener.in HTML page into a fundamentals dict."""
        try:
            soup = BeautifulSoup(html, "lxml")
            data = {}

            # ── Top ratios section ──────────────────────────────────────
            # Screener uses #top-ratios ul > li with .name and .value spans
            for li in soup.select("#top-ratios li"):
                name  = li.select_one(".name")
                value = li.select_one(".value, .number")
                if not (name and value):
                    continue
                name_txt  = name.get_text(strip=True).lower()
                value_txt = value.get_text(strip=True)
                self._map_ratio(data, name_txt, value_txt)

            # ── Quarterly results — EPS ──────────────────────────────────
            eps = self._extract_table_value(soup, "#quarters", "eps", col_idx=-1)
            if eps is not None:
                data["eps"] = eps

            # ── P&L — Revenue ────────────────────────────────────────────
            revenue = self._extract_table_value(soup, "#profit-loss", "sales", col_idx=-1)
            if revenue is not None:
                data["revenue_cr"] = revenue

            revenue_prev = self._extract_table_value(soup, "#profit-loss", "sales", col_idx=-2)
            if revenue is not None and revenue_prev:
                data["revenue_growth"] = (revenue / revenue_prev - 1) * 100

            # ── Net profit margin ─────────────────────────────────────────
            npm = self._extract_table_value(soup, "#profit-loss", "net profit", col_idx=-1)
            if npm is not None and revenue:
                data["net_profit_margin"] = (npm / revenue * 100) if revenue != 0 else None

            # ── Shareholding ─────────────────────────────────────────────
            data.update(self._extract_shareholding(soup))

            if not data:
                logger.debug(f"{symbol}: no data extracted from Screener.in")
                return None

            return data

        except Exception as e:
            logger.debug(f"Parse error for {symbol}: {e}")
            return None

    def _map_ratio(self, data: dict, name: str, value_str: str):
        """Map a Screener.in ratio name to our field name and parse the value."""
        name = name.lower().strip()
        v    = self._parse_number(value_str)

        mapping = {
            "market cap":         "market_cap_cr",
            "current price":      None,  # skip
            "high / low":         None,  # skip
            "stock p/e":          "pe_ratio",
            "p/e":                "pe_ratio",
            "book value":         None,
            "dividend yield":     None,
            "roce":               "roce",
            "roe":                "roe",
            "face value":         None,
            "p/b":                "pb_ratio",
            "price to book":      "pb_ratio",
            "eps":                "eps",
            "debt to equity":     "debt_equity",
            "d/e":                "debt_equity",
            "current ratio":      "current_ratio",
            "quick ratio":        None,
            "interest coverage":  None,
            "sales growth 3years": "revenue_growth_3yr",
            "sales growth":       "revenue_growth",
        }

        for key_pattern, field in mapping.items():
            if key_pattern in name and field:
                if v is not None:
                    data[field] = v
                return

    def _extract_table_value(self, soup, table_selector: str,
                              row_label: str, col_idx: int = -1) -> Optional[float]:
        """Extract a value from a Screener.in data table by row label."""
        table = soup.select_one(table_selector)
        if not table:
            return None

        for row in table.select("tr"):
            cells = row.select("td")
            if not cells:
                continue
            label = cells[0].get_text(strip=True).lower()
            if row_label in label:
                try:
                    target_cell = cells[col_idx] if abs(col_idx) < len(cells) else cells[-1]
                    return self._parse_number(target_cell.get_text(strip=True))
                except (IndexError, ValueError):
                    continue
        return None

    def _extract_shareholding(self, soup) -> dict:
        """Extract Promoter/FII/DII shareholding percentages."""
        data = {}
        table = soup.select_one("#shareholding")
        if not table:
            return data

        label_map = {
            "promoters":  "promoter_holding",
            "fii":        "fii_holding",
            "dii":        "dii_holding",
            "public":     None,
        }

        for row in table.select("tbody tr"):
            cells = row.select("td")
            if not cells:
                continue
            label = cells[0].get_text(strip=True).lower()
            for key, field in label_map.items():
                if key in label and field:
                    # Latest quarter = last cell
                    val = self._parse_number(cells[-1].get_text(strip=True))
                    if val is not None:
                        data[field] = val
                    break

        return data

    @staticmethod
    def _parse_number(text: str) -> Optional[float]:
        """Parse a number string like '₹2,345.67' or '23.5%' → float."""
        if not text:
            return None
        # Remove currency symbols, commas, % signs
        cleaned = re.sub(r"[₹,\s%]", "", text.strip())
        # Handle Cr suffix (crores)
        multiplier = 1.0
        if cleaned.endswith("Cr"):
            multiplier = 1.0
            cleaned = cleaned[:-2]
        try:
            return float(cleaned) * multiplier
        except ValueError:
            return None

    # ── HTTP Fetch ─────────────────────────────────────────────────────────

    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetch HTML with retry logic."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=15)
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code == 404:
                    return None
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited by Screener.in — sleeping 10s")
                    time.sleep(10)
                else:
                    logger.debug(f"HTTP {resp.status_code} for {url}")
                    return None
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    logger.debug(f"HTTP error for {url}: {e}")
                    return None
                time.sleep(2 ** attempt)
        return None

    # ── Storage ────────────────────────────────────────────────────────────

    def _store(self, symbol: str, data: dict):
        """Upsert fundamental data into DuckDB."""
        today = date.today()

        fields = {
            "symbol":           symbol,
            "period_end":       today,
            "period_type":      "quarterly",
            "pe_ratio":         data.get("pe_ratio"),
            "pb_ratio":         data.get("pb_ratio"),
            "market_cap_cr":    data.get("market_cap_cr"),
            "eps":              data.get("eps"),
            "eps_growth_yoy":   data.get("eps_growth_yoy"),
            "roe":              data.get("roe"),
            "roce":             data.get("roce"),
            "debt_equity":      data.get("debt_equity"),
            "current_ratio":    data.get("current_ratio"),
            "revenue_cr":       data.get("revenue_cr"),
            "revenue_growth":   data.get("revenue_growth"),
            "net_profit_margin": data.get("net_profit_margin"),
            "promoter_holding": data.get("promoter_holding"),
            "fii_holding":      data.get("fii_holding"),
            "dii_holding":      data.get("dii_holding"),
            "scraped_at":       datetime.now(),
        }

        cols   = list(fields.keys())
        vals   = list(fields.values())
        col_str = ", ".join(cols)
        ph_str  = ", ".join(["?"] * len(cols))

        self.con.execute(
            f"INSERT OR REPLACE INTO fundamentals ({col_str}) VALUES ({ph_str})",
            vals
        )

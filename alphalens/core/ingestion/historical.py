"""
alphalens/core/ingestion/historical.py

Fetches 15-year historical OHLCV data for all Nifty200 stocks from yfinance.
Runs in batches of 20 symbols to respect rate limits.
Handles incremental updates (only fetch missing dates).

Usage:
    from alphalens.core.ingestion.historical import HistoricalLoader
    loader = HistoricalLoader()
    loader.backfill_all(period="15y")           # Initial 15yr load
    loader.update_incremental()                  # Daily update
    loader.backfill_symbol("RELIANCE", "15y")   # Single stock
"""

import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from loguru import logger

from alphalens.core.database import get_duck
from alphalens.core.ingestion.universe import NIFTY200_UNIVERSE, get_yfinance_symbol

# Also fetch global context symbols
CONTEXT_SYMBOLS = {
    "nifty200":   "^CNX200",
    "nifty50":    "^NSEI",
    "india_vix":  "^INDIAVIX",
    "nifty_bank": "^NSEBANK",
    "nifty_it":   "^CNXIT",
    "nifty_auto": "^CNXAUTO",
    "nifty_fmcg": "^CNXFMCG",
    "nifty_pharma": "^CNXPHARMA",
    "nifty_metal":  "^CNXMETAL",
    "nifty_realty": "^CNXREALTY",
    "nifty_energy": "^CNXENERGY",
    "nifty_infra":  "^CNXINFRA",
    "nifty_psubank": "^CNXPSUBANK",
    "nifty_fin":    "^CNXFINANCE",
    "nifty_media":  "^CNXMEDIA",
    "dxy":         "DX-Y.NYB",
    "brent_crude": "BZ=F",
    "nasdaq":      "^NDX",
    "dow":         "^DJI",
    "us_10yr":     "^TNX",
    "usd_inr":     "USDINR=X",
}

BATCH_SIZE     = 20    # symbols per yfinance.download call
BATCH_DELAY_S  = 2.0   # seconds between batches
MAX_RETRIES    = 3
RETRY_DELAY_S  = 5.0


class HistoricalLoader:

    def __init__(self):
        self.con = get_duck()

    # ── Public Methods ─────────────────────────────────────────────────────

    def backfill_all(self, period: str = "15y") -> dict:
        """
        Full historical backfill for all Nifty200 stocks + global context.
        Returns stats: {total_stocks, total_bars, errors}.
        """
        symbols = [row[0] for row in NIFTY200_UNIVERSE]
        logger.info(f"Starting full backfill: {len(symbols)} stocks, period={period}")

        batches   = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
        stats     = {"total_stocks": 0, "total_bars": 0, "errors": []}

        for idx, batch in enumerate(batches, 1):
            logger.info(f"Batch {idx}/{len(batches)} ({len(batch)} symbols)")
            result = self._fetch_and_store_batch(batch, period=period)
            stats["total_stocks"] += result["stocks"]
            stats["total_bars"]   += result["bars"]
            if result.get("errors"):
                stats["errors"].extend(result["errors"])
            time.sleep(BATCH_DELAY_S)

        # Fetch global context data
        logger.info("Fetching global market context data...")
        self._fetch_context_data(period=period)

        logger.info(
            f"Backfill complete: {stats['total_stocks']} stocks, "
            f"{stats['total_bars']:,} bars, {len(stats['errors'])} errors"
        )
        return stats

    def update_incremental(self, days_back: int = 5) -> dict:
        """
        Incremental update: fetch last N days for all stocks.
        Called by EOD scheduler (6:30 PM daily).
        """
        symbols = [row[0] for row in NIFTY200_UNIVERSE]
        period  = f"{days_back}d"
        logger.info(f"Incremental update: {len(symbols)} stocks, period={period}")

        batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
        stats   = {"total_bars": 0, "errors": []}

        for batch in batches:
            result = self._fetch_and_store_batch(batch, period=period)
            stats["total_bars"] += result["bars"]
            time.sleep(1.0)

        self._fetch_context_data(period=period)
        logger.info(f"Incremental update complete: {stats['total_bars']} bars upserted")
        return stats

    def backfill_symbol(self, symbol: str, period: str = "15y") -> int:
        """Backfill a single symbol. Returns number of bars stored."""
        yf_sym = get_yfinance_symbol(symbol)
        logger.info(f"Backfilling {symbol} ({yf_sym}) period={period}")

        df = self._fetch_single(yf_sym, period)
        if df is None or df.empty:
            logger.warning(f"No data for {symbol}")
            return 0

        count = self._store_prices(symbol, df)
        logger.info(f"{symbol}: stored {count} bars")
        return count

    def get_last_date(self, symbol: str) -> Optional[date]:
        """Return the most recent date in DB for a symbol."""
        result = self.con.execute(
            "SELECT MAX(date) FROM daily_prices WHERE symbol = ?", [symbol]
        ).fetchone()
        return result[0] if result and result[0] else None

    def get_price_count(self, symbol: str) -> int:
        result = self.con.execute(
            "SELECT COUNT(*) FROM daily_prices WHERE symbol = ?", [symbol]
        ).fetchone()
        return result[0] if result else 0

    # ── Private Methods ────────────────────────────────────────────────────

    def _fetch_and_store_batch(self, symbols: list[str], period: str) -> dict:
        """Download a batch of symbols and store to DuckDB."""
        yf_symbols = [get_yfinance_symbol(s) for s in symbols]
        sym_map    = {get_yfinance_symbol(s): s for s in symbols}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                df = yf.download(
                    tickers   = yf_symbols,
                    period    = period,
                    interval  = "1d",
                    auto_adjust = True,
                    actions   = False,
                    group_by  = "ticker",
                    threads   = True,
                    progress  = False,
                )
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    logger.error(f"Batch download failed after {MAX_RETRIES} attempts: {e}")
                    return {"stocks": 0, "bars": 0, "errors": [str(e)]}
                logger.warning(f"Attempt {attempt} failed: {e} — retrying in {RETRY_DELAY_S}s")
                time.sleep(RETRY_DELAY_S)

        total_stocks = 0
        total_bars   = 0
        errors       = []

        for yf_sym, nse_sym in sym_map.items():
            try:
                if len(yf_symbols) == 1:
                    sym_df = df.copy()
                else:
                    sym_df = df[yf_sym].copy() if yf_sym in df.columns.get_level_values(0) else pd.DataFrame()

                if sym_df.empty or "Close" not in sym_df.columns:
                    logger.warning(f"No data returned for {nse_sym} ({yf_sym})")
                    errors.append(f"no_data:{nse_sym}")
                    continue

                sym_df = sym_df.dropna(subset=["Close"])
                count  = self._store_prices(nse_sym, sym_df)
                total_stocks += 1
                total_bars   += count

            except Exception as e:
                logger.warning(f"Failed to process {nse_sym}: {e}")
                errors.append(f"{nse_sym}:{e}")

        return {"stocks": total_stocks, "bars": total_bars, "errors": errors}

    def _fetch_single(self, yf_symbol: str, period: str) -> Optional[pd.DataFrame]:
        """Fetch single symbol with retry."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                ticker = yf.Ticker(yf_symbol)
                df     = ticker.history(
                    period      = period,
                    interval    = "1d",
                    auto_adjust = True,
                    actions     = False
                )
                return df
            except Exception as e:
                if attempt == MAX_RETRIES:
                    logger.error(f"Failed to fetch {yf_symbol}: {e}")
                    return None
                time.sleep(RETRY_DELAY_S)
        return None

    def _store_prices(self, symbol: str, df: pd.DataFrame) -> int:
        """Store OHLCV dataframe to DuckDB daily_prices table. Returns rows inserted."""
        if df.empty:
            return 0

        # Normalise index
        df.index = pd.to_datetime(df.index)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # Build records
        records = []
        for dt, row in df.iterrows():
            try:
                records.append((
                    dt.date(),                                # date
                    symbol,                                    # symbol
                    float(row.get("Open",  row["Close"])),    # open
                    float(row.get("High",  row["Close"])),    # high
                    float(row.get("Low",   row["Close"])),    # low
                    float(row["Close"]),                       # close
                    float(row.get("Adj Close", row["Close"])),# adj_close
                    int(row.get("Volume", 0)),                 # volume
                    "yfinance",                                # source
                ))
            except (KeyError, ValueError, TypeError):
                continue

        if not records:
            return 0

        # Upsert into DuckDB
        self.con.executemany("""
            INSERT OR REPLACE INTO daily_prices
            (date, symbol, open, high, low, close, adj_close, volume, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

        return len(records)

    def _fetch_context_data(self, period: str = "15y"):
        """Fetch all global/macro context symbols and store to market_context."""
        context_dfs = {}

        for name, yf_sym in CONTEXT_SYMBOLS.items():
            try:
                ticker = yf.Ticker(yf_sym)
                df     = ticker.history(period=period, interval="1d",
                                        auto_adjust=True, actions=False)
                if df.empty:
                    logger.warning(f"No data for context symbol {name} ({yf_sym})")
                    continue
                df.index = pd.to_datetime(df.index)
                if df.index.tz:
                    df.index = df.index.tz_localize(None)
                context_dfs[name] = df["Close"].rename(name)
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to fetch context {name}: {e}")

        if not context_dfs:
            return

        # Align all series on common date index
        combined = pd.concat(context_dfs.values(), axis=1)
        combined.index.name = "date"
        combined = combined.reset_index()

        # Map column names to market_context columns
        col_map = {
            "nifty200":    "nifty200_close",
            "india_vix":   "india_vix",
            "dxy":         "dxy",
            "brent_crude": "brent_crude",
            "nasdaq":      "nasdaq_close",
            "dow":         "dow_close",
            "us_10yr":     "us_10yr_yield",
            "usd_inr":     "usd_inr",
            "nifty_bank":  "sector_bank_close",
            "nifty_it":    "sector_it_close",
            "nifty_auto":  "sector_auto_close",
            "nifty_fmcg":  "sector_fmcg_close",
            "nifty_pharma": "sector_pharma_close",
            "nifty_metal":  "sector_metal_close",
            "nifty_realty": "sector_realty_close",
            "nifty_energy": "sector_energy_close",
            "nifty_infra":  "sector_infra_close",
            "nifty_psubank": "sector_psubank_close",
            "nifty_fin":    "sector_fin_close",
            "nifty_media":  "sector_media_close",
        }
        combined = combined.rename(columns=col_map)

        # Compute derived fields
        if "nifty200_close" in combined.columns:
            combined["nifty200_1d_ret"]  = combined["nifty200_close"].pct_change(1) * 100
            combined["nifty200_5d_ret"]  = combined["nifty200_close"].pct_change(5) * 100
            combined["nifty200_20d_ret"] = combined["nifty200_close"].pct_change(20) * 100

        if "nasdaq_close" in combined.columns:
            combined["nasdaq_5d_ret"] = combined["nasdaq_close"].pct_change(5) * 100
        if "dow_close" in combined.columns:
            combined["dow_5d_ret"] = combined["dow_close"].pct_change(5) * 100
        if "india_vix" in combined.columns:
            combined["india_vix_1d_chg"] = combined["india_vix"].pct_change(1) * 100
            combined["india_vix_pct252"] = (
                combined["india_vix"]
                .rolling(252, min_periods=50)
                .rank(pct=True) * 100
            )

        combined = combined.dropna(subset=["date"])
        combined["date"] = pd.to_datetime(combined["date"]).dt.date

        # Upsert to market_context
        cols = [c for c in combined.columns if c in self._context_columns()]
        if not cols:
            return

        combined_cols = ["date"] + [c for c in cols if c != "date"]
        combined_sub  = combined[combined_cols].dropna(subset=["date"])

        placeholders = ", ".join(["?"] * len(combined_cols))
        col_str      = ", ".join(combined_cols)

        self.con.executemany(
            f"INSERT OR REPLACE INTO market_context ({col_str}) VALUES ({placeholders})",
            combined_sub.values.tolist()
        )
        logger.info(f"Market context updated: {len(combined_sub)} rows")

    def _context_columns(self) -> set:
        """Return all column names in market_context table."""
        cols = self.con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='market_context'"
        ).fetchall()
        return {c[0] for c in cols}

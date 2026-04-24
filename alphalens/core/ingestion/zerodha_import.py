"""
alphalens/core/ingestion/zerodha_import.py

Imports Zerodha portfolio data from CSV exports into the portfolio_holdings
and closed_trades tables in SQLite.

Supported formats:
  1. Holdings CSV  — downloaded from Zerodha Console → Portfolio → Holdings
     Columns: Instrument, Qty, Avg cost, LTP, Cur val, P&L, Net chg, Day chg

  2. Tradebook CSV — downloaded from Zerodha Console → Reports → Tradebook
     Columns: symbol, isin, trade_date, exchange, segment, series,
              trade_type, quantity, price, trade_id, order_id, order_execution_time

Usage:
    importer = ZerodhaImporter()

    # Holdings
    result = importer.import_holdings("/path/to/holdings.csv", timeframe="long_term")

    # Tradebook
    result = importer.import_tradebook("/path/to/tradebook.csv")

    # From bytes (Dash Upload component)
    content = base64.b64decode(upload_contents.split(",")[1])
    result = importer.import_holdings_bytes(content, timeframe="swing")
"""

import io
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from loguru import logger

from alphalens.core.database import get_sqlite, PortfolioHolding, ClosedTrade, ZerodhaImport


# ── Zerodha Holdings CSV Columns ──────────────────────────────────────────
# Zerodha exports holdings with these columns (as of 2024)
HOLDINGS_COLUMNS = {
    "required": ["Instrument", "Qty", "Avg cost"],
    "optional": ["LTP", "Cur val", "P&L", "Net chg", "Day chg"],
}

# Zerodha Tradebook CSV columns
TRADEBOOK_COLUMNS = {
    "required": ["symbol", "trade_date", "trade_type", "quantity", "price"],
    "optional": ["isin", "exchange", "segment", "series", "trade_id", "order_id",
                 "order_execution_time"],
}


class ZerodhaImporter:

    # ── Holdings Import ────────────────────────────────────────────────────

    def import_holdings(self, file_path: Union[str, Path],
                         timeframe: str = "long_term") -> dict:
        """
        Import Zerodha Holdings CSV into portfolio_holdings table.

        Args:
            file_path: Path to the CSV file
            timeframe: Which portfolio slot to assign (long_term | swing | medium | intraday)

        Returns:
            dict with keys: imported, skipped, errors, holdings
        """
        try:
            df = pd.read_csv(file_path)
            return self._process_holdings_df(df, timeframe, str(file_path))
        except Exception as e:
            logger.error(f"Holdings import failed: {e}")
            return {"error": str(e), "imported": 0, "skipped": 0}

    def import_holdings_bytes(self, content: bytes,
                               timeframe: str = "long_term",
                               filename: str = "upload.csv") -> dict:
        """Import holdings from raw bytes (from Dash upload component)."""
        try:
            df = pd.read_csv(io.BytesIO(content))
            return self._process_holdings_df(df, timeframe, filename)
        except Exception as e:
            logger.error(f"Holdings import (bytes) failed: {e}")
            return {"error": str(e), "imported": 0, "skipped": 0}

    def _process_holdings_df(self, df: pd.DataFrame,
                               timeframe: str, filename: str) -> dict:
        """Process a holdings DataFrame and insert into SQLite."""
        # Normalise column names (strip whitespace)
        df.columns = df.columns.str.strip()

        # Validate required columns
        missing = [c for c in HOLDINGS_COLUMNS["required"] if c not in df.columns]
        if missing:
            # Try to detect alternate column names
            df = self._try_rename_holdings_cols(df)
            missing = [c for c in HOLDINGS_COLUMNS["required"] if c not in df.columns]
            if missing:
                return {"error": f"Missing columns: {missing}", "imported": 0, "skipped": 0}

        imported, skipped, errors = 0, 0, []
        holdings_list = []
        today = date.today()

        with get_sqlite() as session:
            for _, row in df.iterrows():
                try:
                    symbol = str(row["Instrument"]).strip().upper()
                    if not symbol or symbol.lower() in ("nan", "total", ""):
                        skipped += 1
                        continue

                    qty = self._parse_int(row.get("Qty", 0))
                    if qty <= 0:
                        skipped += 1
                        continue

                    avg_cost = self._parse_float(row.get("Avg cost", 0))
                    if avg_cost <= 0:
                        skipped += 1
                        continue

                    ltp = self._parse_float(row.get("LTP", avg_cost))

                    # Check for existing holding
                    existing = session.query(PortfolioHolding).filter(
                        PortfolioHolding.symbol == symbol,
                        PortfolioHolding.timeframe == timeframe,
                        PortfolioHolding.is_active == True
                    ).first()

                    if existing:
                        # Update quantity and avg cost (weighted average)
                        total_qty  = existing.qty + qty
                        new_avg    = (existing.avg_cost * existing.qty + avg_cost * qty) / total_qty
                        existing.qty      = total_qty
                        existing.avg_cost = new_avg
                        existing.updated_at = datetime.now()
                        imported += 1
                    else:
                        holding = PortfolioHolding(
                            symbol      = symbol,
                            timeframe   = timeframe,
                            qty         = qty,
                            avg_cost    = avg_cost,
                            entry_date  = today,
                            source      = "zerodha_csv",
                            is_active   = True,
                            created_at  = datetime.now(),
                            updated_at  = datetime.now(),
                        )
                        session.add(holding)
                        imported += 1

                    holdings_list.append({
                        "symbol":   symbol,
                        "qty":      qty,
                        "avg_cost": avg_cost,
                        "ltp":      ltp,
                        "pnl":      (ltp - avg_cost) * qty,
                        "pnl_pct":  (ltp / avg_cost - 1) * 100 if avg_cost > 0 else 0,
                    })

                except Exception as e:
                    logger.debug(f"Row error: {e}")
                    errors.append(str(e))

            # Log the import
            session.add(ZerodhaImport(
                import_type   = "holdings",
                filename      = filename,
                rows_imported = imported,
                imported_at   = datetime.now(),
                notes         = f"timeframe={timeframe}"
            ))

        logger.info(f"Holdings import: {imported} imported, {skipped} skipped, {len(errors)} errors")
        return {
            "imported": imported,
            "skipped":  skipped,
            "errors":   errors,
            "holdings": holdings_list,
        }

    # ── Tradebook Import ───────────────────────────────────────────────────

    def import_tradebook(self, file_path: Union[str, Path],
                          default_timeframe: str = "swing") -> dict:
        """
        Import Zerodha Tradebook CSV.

        Tradebook contains individual buy/sell transactions.
        We match buys with sells to reconstruct closed trades with P&L.
        Open positions (unmatched buys) are added to portfolio_holdings.
        """
        try:
            df = pd.read_csv(file_path)
            return self._process_tradebook_df(df, default_timeframe, str(file_path))
        except Exception as e:
            logger.error(f"Tradebook import failed: {e}")
            return {"error": str(e), "imported": 0}

    def import_tradebook_bytes(self, content: bytes,
                                default_timeframe: str = "swing",
                                filename: str = "tradebook.csv") -> dict:
        """Import tradebook from raw bytes."""
        try:
            df = pd.read_csv(io.BytesIO(content))
            return self._process_tradebook_df(df, default_timeframe, filename)
        except Exception as e:
            return {"error": str(e), "imported": 0}

    def _process_tradebook_df(self, df: pd.DataFrame,
                                default_timeframe: str, filename: str) -> dict:
        """Match buys/sells and create closed_trades + open portfolio positions."""
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        # Normalise column names
        rename_map = {
            "tradingsymbol": "symbol", "instrument": "symbol",
            "buy_quantity":  "quantity", "sell_quantity": "quantity",
            "buy_price":     "price",    "sell_price":    "price",
        }
        df = df.rename(columns=rename_map)

        required = ["symbol", "trade_date", "trade_type", "quantity", "price"]
        missing  = [c for c in required if c not in df.columns]
        if missing:
            return {"error": f"Missing columns: {missing}", "imported": 0}

        # Parse dates
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["trade_date"])
        df["trade_date"] = df["trade_date"].dt.date

        # Normalise trade type
        df["trade_type"] = df["trade_type"].str.upper().str.strip()
        df["quantity"]   = df["quantity"].apply(self._parse_int)
        df["price"]      = df["price"].apply(self._parse_float)
        df = df.sort_values("trade_date")

        # ── FIFO matching per symbol ─────────────────────────────────────
        symbols        = df["symbol"].str.upper().str.strip().unique()
        closed_trades  = []
        open_positions = []

        for symbol in symbols:
            sym_df   = df[df["symbol"].str.upper() == symbol].copy()
            buys     = sym_df[sym_df["trade_type"] == "BUY"].to_dict("records")
            sells    = sym_df[sym_df["trade_type"] == "SELL"].to_dict("records")

            buy_queue = list(buys)   # FIFO queue
            remaining = list(buys)

            for sell in sells:
                sell_qty   = sell["quantity"]
                sell_price = sell["price"]
                sell_date  = sell["trade_date"]

                while sell_qty > 0 and buy_queue:
                    buy         = buy_queue[0]
                    matched_qty = min(buy["quantity"], sell_qty)

                    pnl     = (sell_price - buy["price"]) * matched_qty
                    pnl_pct = (sell_price / buy["price"] - 1) * 100 if buy["price"] > 0 else 0
                    days    = (sell_date - buy["trade_date"]).days if isinstance(sell_date, date) and isinstance(buy["trade_date"], date) else 0
                    tax_type = "LTCG" if days > 365 else "STCG"

                    closed_trades.append({
                        "symbol":      symbol,
                        "timeframe":   self._infer_timeframe(days, default_timeframe),
                        "qty":         matched_qty,
                        "entry_date":  buy["trade_date"],
                        "entry_price": buy["price"],
                        "exit_date":   sell_date,
                        "exit_price":  sell_price,
                        "booked_pnl":  pnl,
                        "booked_pnl_pct": pnl_pct,
                        "holding_days": days,
                        "tax_type":    tax_type,
                        "exit_reason": "tradebook_import",
                    })

                    buy["quantity"] -= matched_qty
                    sell_qty        -= matched_qty

                    if buy["quantity"] == 0:
                        buy_queue.pop(0)

            # Remaining unmatched buys = open positions
            for buy in buy_queue:
                if buy["quantity"] > 0:
                    open_positions.append({
                        "symbol":    symbol,
                        "qty":       buy["quantity"],
                        "avg_cost":  buy["price"],
                        "entry_date": buy["trade_date"],
                        "timeframe": default_timeframe,
                    })

        # ── Persist ────────────────────────────────────────────────────────
        trades_added   = 0
        holdings_added = 0

        with get_sqlite() as session:
            for t in closed_trades:
                session.add(ClosedTrade(
                    symbol          = t["symbol"],
                    timeframe       = t["timeframe"],
                    qty             = t["qty"],
                    entry_date      = t["entry_date"],
                    entry_price     = t["entry_price"],
                    exit_date       = t["exit_date"],
                    exit_price      = t["exit_price"],
                    booked_pnl      = t["booked_pnl"],
                    booked_pnl_pct  = t["booked_pnl_pct"],
                    holding_days    = t["holding_days"],
                    tax_type        = t["tax_type"],
                    exit_reason     = t["exit_reason"],
                    created_at      = datetime.now(),
                ))
                trades_added += 1

            for p in open_positions:
                existing = session.query(PortfolioHolding).filter(
                    PortfolioHolding.symbol    == p["symbol"],
                    PortfolioHolding.timeframe == p["timeframe"],
                    PortfolioHolding.is_active == True
                ).first()

                if not existing:
                    session.add(PortfolioHolding(
                        symbol      = p["symbol"],
                        timeframe   = p["timeframe"],
                        qty         = p["qty"],
                        avg_cost    = p["avg_cost"],
                        entry_date  = p["entry_date"],
                        source      = "zerodha_tradebook",
                        is_active   = True,
                        created_at  = datetime.now(),
                        updated_at  = datetime.now(),
                    ))
                    holdings_added += 1

            session.add(ZerodhaImport(
                import_type   = "tradebook",
                filename      = filename,
                rows_imported = trades_added + holdings_added,
                imported_at   = datetime.now(),
                notes         = f"closed_trades={trades_added}, open_holdings={holdings_added}"
            ))

        logger.info(
            f"Tradebook import: {trades_added} closed trades, "
            f"{holdings_added} open positions for {len(symbols)} symbols"
        )
        return {
            "closed_trades":  trades_added,
            "open_positions": holdings_added,
            "symbols":        len(symbols),
        }

    # ── Validation & Preview ───────────────────────────────────────────────

    def validate_holdings_csv(self, content: bytes) -> dict:
        """Validate a holdings CSV before importing. Returns preview."""
        try:
            df = pd.read_csv(io.BytesIO(content))
            df.columns = df.columns.str.strip()

            missing = [c for c in HOLDINGS_COLUMNS["required"] if c not in df.columns]
            if missing:
                df = self._try_rename_holdings_cols(df)
                missing = [c for c in HOLDINGS_COLUMNS["required"] if c not in df.columns]

            return {
                "valid":         len(missing) == 0,
                "missing_cols":  missing,
                "row_count":     len(df),
                "columns":       list(df.columns),
                "preview":       df.head(5).to_dict("records"),
            }
        except Exception as e:
            return {"valid": False, "error": str(e)}

    def validate_tradebook_csv(self, content: bytes) -> dict:
        """Validate a tradebook CSV before importing."""
        try:
            df = pd.read_csv(io.BytesIO(content))
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

            required = ["symbol", "trade_date", "trade_type", "quantity", "price"]
            missing  = [c for c in required if c not in df.columns]

            return {
                "valid":         len(missing) == 0,
                "missing_cols":  missing,
                "row_count":     len(df),
                "columns":       list(df.columns),
                "preview":       df.head(5).to_dict("records"),
            }
        except Exception as e:
            return {"valid": False, "error": str(e)}

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _try_rename_holdings_cols(df: pd.DataFrame) -> pd.DataFrame:
        """Try to auto-detect alternate column names in holdings CSV."""
        rename = {}
        lower_cols = {c.lower().strip(): c for c in df.columns}

        col_alternatives = {
            "Instrument": ["stock", "symbol", "scrip", "tradingsymbol", "instrument"],
            "Qty":        ["quantity", "shares", "holding qty"],
            "Avg cost":   ["avg. cost", "average cost", "avg cost price", "average price", "cost price"],
        }

        for target, alternatives in col_alternatives.items():
            if target not in df.columns:
                for alt in alternatives:
                    if alt.lower() in lower_cols:
                        rename[lower_cols[alt.lower()]] = target
                        break

        return df.rename(columns=rename)

    @staticmethod
    def _infer_timeframe(holding_days: int, default: str) -> str:
        """Infer trading timeframe from holding duration."""
        if holding_days == 0:
            return "intraday"
        elif holding_days <= 10:
            return "swing"
        elif holding_days <= 60:
            return "medium"
        else:
            return "long_term"

    @staticmethod
    def _parse_int(val) -> int:
        try:
            return int(float(str(val).replace(",", "").strip()))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _parse_float(val) -> float:
        try:
            return float(str(val).replace(",", "").replace("₹", "").strip())
        except (ValueError, TypeError):
            return 0.0

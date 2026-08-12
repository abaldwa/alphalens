"""
Tickers deliberately withheld from the BACKTEST trade universe.

Why this exists
---------------
The 2026-08-11/12 Fyers-primary backfill corrected ~1.5M ticker-days of
2007-2016 OHLCV whose adjustment factors had manufactured impossible returns
(BAJFINANCE entered at Rs 0.04 in 2010 against a real Rs 3.16, fabricating
+18,610%, which a lump-capital backtest then compounded into every later
trade). It did not correct everything: roughly 42% of 2007-2016 is still
legacy source=NULL data for ~1,626 tickers Fyers will not serve, and ~214
adjacent-day price jumps survive across ~183 tickers.

Most of those survivors are legitimate — demergers that no vendor adjusts
(SUVEN 2020, GFLLIMITED 2019, ADANIENT 2015, NIITLTD 2023) — and the handful
of genuine bad prints are one-day round trips. The standing decision is
therefore to run the full universe and check afterwards whether any strategy's
results actually LEAN on the unverifiable names, rather than pre-emptively
dropping them.

That check can come back positive. This module is the lever for that case:
a reviewed, documented exclusion list, empty by default.

Deliberately narrow scope
-------------------------
This filter applies ONLY to the backtest's tradeable universe. It is NOT
applied in config/universe.py, because that module also drives ingestion and
feature computation — we still want to INGEST these tickers (their data is
how we'd ever verify or repair them); we just may not want to TRADE them in a
historical simulation.

Excluding tickers is not free. Dropping names whose numbers look implausible
is precisely how a backtest is made to flatter itself, and it reintroduces
the survivorship bias the PIT universe exists to avoid. Every entry must
therefore carry a concrete reason, and exclusions should be justified by
"this ticker's PRICE HISTORY is unverifiable", never by "this ticker's
RETURNS are inconvenient".
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Set

logger = logging.getLogger(__name__)

EXCLUSIONS_PATH = Path(__file__).resolve().parent / "backtest_excluded_tickers.json"

# Escape hatch for a one-off run without editing the committed file:
#   ALPHALENS_BACKTEST_EXCLUDE="BTML,TPHQ,AKI"
# Comma-separated, unioned with the file's contents.
_ENV_VAR = "ALPHALENS_BACKTEST_EXCLUDE"


def load_exclusions() -> Dict[str, str]:
    """{ticker: reason} for every ticker withheld from the backtest universe.

    Returns an empty dict when no exclusion file exists — the default, and
    the state the 2007-start rebuild is intended to run in.
    """
    out: Dict[str, str] = {}

    if EXCLUSIONS_PATH.exists():
        try:
            raw = json.loads(EXCLUSIONS_PATH.read_text())
        except (OSError, ValueError) as exc:
            # Fail loudly rather than silently trading a universe the user
            # believes is filtered.
            raise ValueError(f"could not parse {EXCLUSIONS_PATH}: {exc}") from exc

        entries = raw.get("excluded", raw) if isinstance(raw, dict) else raw
        if isinstance(entries, dict):
            out.update({str(k).upper(): str(v) for k, v in entries.items()})
        else:
            for e in entries:
                if isinstance(e, str):
                    out[e.upper()] = "(no reason recorded)"
                else:
                    out[str(e["ticker"]).upper()] = str(e.get("reason", "(no reason recorded)"))

    env = os.environ.get(_ENV_VAR, "").strip()
    if env:
        for t in (x.strip().upper() for x in env.split(",")):
            if t:
                out.setdefault(t, f"{_ENV_VAR} environment override")

    return out


def excluded_tickers() -> Set[str]:
    """Just the ticker set, for membership tests."""
    return set(load_exclusions())


def apply_exclusions(tickers: List[str], context: str = "backtest") -> List[str]:
    """Drop excluded tickers from `tickers`, logging exactly what was removed.

    Logs at WARNING when anything is dropped: an excluded universe is a
    material caveat on every number the run produces, and it must be visible
    in the run's own log rather than only in a config file nobody re-reads.
    Order is otherwise preserved (callers rely on ADTV-descending order).
    """
    ex = load_exclusions()
    if not ex:
        return tickers

    kept = [t for t in tickers if t.upper() not in ex]
    dropped = [t for t in tickers if t.upper() in ex]
    if dropped:
        logger.warning(
            "%s universe: EXCLUDING %d/%d tickers by config/backtest_excluded_tickers.json — %s",
            context, len(dropped), len(tickers),
            "; ".join(f"{t} ({ex[t.upper()]})" for t in dropped),
        )
    return kept

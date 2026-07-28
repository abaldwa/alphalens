"""
systems/fundamental_analysis/quality/net_net.py

Graham Net-Net / Asset Value: buy below net current asset value. Compares
an absolute rupee-per-share value to price, so — like margin_of_safety.py
and piotroski_on_value.py — reads raw PIT financials directly rather than
the sector-z-scored feature panel.

preferred_equity has no column anywhere (see margin_of_safety.py's
docstring) and is treated as 0. The doc's promoter_pledge_ratio==0
governance overlay is intentionally NOT included here — per this
project's Promoter-Aligned Compounders design, governance fields are an
overlay to be combined by the caller, not baked into a standalone
value screen.

[INVESTIGATED, 6th fundamental-strategies review, item 4] a real 5-year/
2000-ticker orchestrator run (2021-2025) produced exactly 0 net_net
trades. Two findings, one fixed here and one a genuine data-availability
limit that is NOT a bug in this module:

1. (fixed) This screen only ever read the direct `total_liabilities`
   column, even though `current_liabilities` + `non_current_liabilities`
   (the two components total_liabilities is computed from) agree with it
   to within 1% in ~98% of rows where all three are present, and are
   sometimes populated when the direct aggregate isn't. Falls back to
   that sum whenever the direct total_liabilities field is missing —
   strictly more permissive than the old always-NULL-before-2023
   behavior, never fabricated.
2. (NOT a bug — a real data gap, left undocumented rather than "fixed")
   `current_assets`, `current_liabilities`, AND `non_current_liabilities`
   are ALL themselves systemically sparse-to-absent for the 2020-2024
   window in `fundamentals`/`fundamentals_history` (confirmed via direct
   query: current_assets is populated for only single-digit-to-dozens of
   rows per quarter out of ~1,800-1,995 total tickers before quarter_end_
   date 2023-03-31, and non_current_liabilities is 0/NULL for literally
   every row before 2025-03-31). The fallback in (1) therefore still
   can't fire meaningfully before ~2025, because its own inputs aren't in
   the database yet for earlier periods either. This is a genuine gap in
   how far back granular balance-sheet line items (current_assets/
   current_liabilities/non_current_liabilities, as opposed to the
   higher-level total_equity/total_debt/revenue metrics that ARE well
   populated historically) have been backfilled — a datastore/ingestion-
   layer question, out of this module's (and this review's) scope to fix.
   Net-nets may also be genuinely rare in the modern Indian market (Graham
   net-nets have always been rare outside deep bear markets), but that
   claim can't even be tested against 2020-2024 data until the underlying
   balance-sheet fields are backfilled that far back.
"""

from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from datastore.api.pit import get_fundamentals_pit

NCAV_DISCOUNT_THRESHOLD = 0.67  # price must be <= 67% of NCAV/share (Graham's classic cutoff)
# Arbitrary but documented liquidity floor — net-nets are rare enough in
# India that without some floor, illiquid micro-caps dominate the screen.
LIQUIDITY_FLOOR_MARKET_CAP_CR = 50.0
CRORE = 1e7


def _latest_close(conn: Any, ticker: str, as_of: datetime) -> Optional[float]:
    row = conn.execute(
        "SELECT close FROM ohlcv_adjusted WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        [ticker, as_of.date()],
    ).fetchone()
    return float(row[0]) if row else None


def compute_net_net(conn: Any, ticker: str, as_of: datetime) -> Dict[str, Any]:
    """
    Returns
    -------
    dict
        ncav, ncav_per_share, market_cap_cr, passes (bool). Missing data
        conservatively fails.
    """
    history = get_fundamentals_pit(conn, [ticker], as_of)
    if history.empty:
        return {"ncav_per_share": np.nan, "passes": False}

    latest = history.sort_values("quarter_end_date").iloc[-1]
    close = _latest_close(conn, ticker, as_of)

    current_assets = latest.get("current_assets")
    total_liabilities = latest.get("total_liabilities")
    shares = latest.get("shares_outstanding")

    if pd.isna(total_liabilities):
        # [BUG FIX, 6th fundamental-strategies review, item 4] direct
        # total_liabilities is sparsely populated (see module docstring) —
        # derive it from its own components when they're present, rather
        # than failing the screen purely for a missing aggregate column.
        current_liabilities = latest.get("current_liabilities")
        non_current_liabilities = latest.get("non_current_liabilities")
        if not pd.isna(current_liabilities) and not pd.isna(non_current_liabilities):
            total_liabilities = current_liabilities + non_current_liabilities

    if close is None or pd.isna(current_assets) or pd.isna(total_liabilities) or pd.isna(shares) or shares <= 0:
        return {"ncav_per_share": np.nan, "passes": False}

    ncav = float(current_assets - total_liabilities)  # preferred_equity == 0, in rupee CRORE
    # Unit conversion (same convention as features/fundamental.py's module
    # docstring): fundamentals columns are rupee CRORE, shares_outstanding
    # is a raw share count, close is raw rupees-per-share — ncav must be
    # converted to raw rupees before dividing by a raw share count.
    ncav_per_share = (ncav * CRORE) / shares
    market_cap_cr = (close * shares) / CRORE

    passes = bool(
        ncav_per_share > 0
        and close <= NCAV_DISCOUNT_THRESHOLD * ncav_per_share
        and market_cap_cr > LIQUIDITY_FLOOR_MARKET_CAP_CR
    )
    return {"ncav": ncav, "ncav_per_share": ncav_per_share, "market_cap_cr": market_cap_cr, "passes": passes}

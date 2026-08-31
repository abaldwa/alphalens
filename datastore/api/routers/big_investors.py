"""
datastore/api/routers/big_investors.py

Phase A/B/C/D: Bulk/Block Deals + MF Holdings — "Big Investor Activity" feature.
Phase A endpoints read the raw `large_deals` table as-is. Phase B adds
family-attributed, netted endpoints reading `bulk_deal_positions`
(ingestion/scrapers/bulk_deal_attribution.py's derived output, rebuilt
daily by the attribute_bulk_deals scheduler step). Phase C adds MF-holdings
movers/entries-exits endpoints reading `mf_holdings` (synced from parquet
by ingestion/scrapers/amfi_holdings.sync_duckdb_table, weekly). Phase D
adds a read/resolve API over `bulk_deal_reconciliation_log`, populated by
scripts/reconcile_bulk_deal_families.py (manual/quarterly, not scheduler-
wired — see that script's module docstring). All join
stock_master.market_cap_cr to classify each ticker into a fixed Rs.
crore cap band per config.settings.BIG_INVESTOR_CAP_*.

PIT (Phase C): mf_holdings.availability_date is the PIT gate (never
`month` itself) — movers/entries-exits only ever compare the two most
recent months whose availability_date <= as_of (default: today), same
discipline as shareholding.filing_date.
"""

import logging
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config.settings import (
    BIG_INVESTOR_CAP_LARGE_CR,
    BIG_INVESTOR_CAP_MID_CR,
    BIG_INVESTOR_CAP_SMALL_CR,
    DUCKDB_PATH,
)
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/big-investors", tags=["Big Investors"])

_DEAL_COLUMNS = [
    "trade_date", "exchange", "deal_type", "ticker", "client_name",
    "transaction_type", "quantity", "price", "remarks",
]
_DEAL_SELECT_COLS = ", ".join(f"ld.{c}" for c in _DEAL_COLUMNS)


def _cap_band(market_cap_cr: Optional[float]) -> str:
    if market_cap_cr is None:
        return "unknown"
    if market_cap_cr > BIG_INVESTOR_CAP_LARGE_CR:
        return "large"
    if market_cap_cr > BIG_INVESTOR_CAP_MID_CR:
        return "mid"
    if market_cap_cr > BIG_INVESTOR_CAP_SMALL_CR:
        return "small"
    return "micro"


def _deal_row_to_dict(row) -> dict:
    d = dict(zip(_DEAL_COLUMNS, row[: len(_DEAL_COLUMNS)]))
    company_name, market_cap_cr = row[len(_DEAL_COLUMNS)], row[len(_DEAL_COLUMNS) + 1]
    d["trade_date"] = d["trade_date"].isoformat() if d["trade_date"] else None
    d["company_name"] = company_name
    d["market_cap_cr"] = market_cap_cr
    d["cap_band"] = _cap_band(market_cap_cr)
    return d


@router.get("/bulk-deals/entries-exits")
async def get_bulk_deal_entries_exits(
    trade_date_param: date = Query(..., alias="date", description="Trade date to list deals for"),
    cap_band: Optional[str] = Query(None, description="Filter: large|mid|small|micro|unknown"),
    exchange: Optional[str] = Query(None, description="Filter: NSE|BSE"),
    deal_type: Optional[str] = Query(None, description="Filter: bulk|block"),
) -> dict:
    """
    Raw bulk/block deals on a given trade_date, cap-band annotated.

    [AS BUILT, Phase A] Registered before /bulk-deals/{ticker} — same
    route-ordering discipline as main.py's forensic/signals note: a literal
    "/bulk-deals/entries-exits" path would otherwise be swallowed by
    get_bulk_deals(ticker="entries-exits") if that wildcard route were
    registered first, since FastAPI matches by registration order.

    No family attribution or intraday netting yet — every client_name row
    from NSE/BSE is shown as-is. Phase B adds investor_family joins and
    wash-trade netting on top of this same data.
    """
    where = ["ld.trade_date = ?"]
    params: list = [trade_date_param]
    if exchange:
        where.append("ld.exchange = ?")
        params.append(exchange)
    if deal_type:
        where.append("ld.deal_type = ?")
        params.append(deal_type)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_DEAL_SELECT_COLS}, sm.company_name, sm.market_cap_cr
            FROM large_deals ld
            LEFT JOIN stock_master sm ON sm.ticker = ld.ticker
            WHERE {' AND '.join(where)}
            ORDER BY sm.market_cap_cr ASC NULLS LAST
            """,
            params,
        ).fetchall()

    data = [_deal_row_to_dict(r) for r in rows]
    if cap_band:
        data = [d for d in data if d["cap_band"] == cap_band]
    return {"date": trade_date_param.isoformat(), "data": data, "record_count": len(data)}


@router.get("/bulk-deals/{ticker}")
async def get_bulk_deals(
    ticker: str,
    start_date: Optional[datetime] = Query(None, description="trade_date range start (inclusive)"),
    end_date: Optional[datetime] = Query(None, description="trade_date range end (inclusive)"),
) -> dict:
    """Raw bulk/block deals for a single ticker, cap-band annotated."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    where = ["ld.ticker = ?"]
    params: list = [ticker]
    if start_date is not None:
        where.append("ld.trade_date >= ?")
        params.append(start_date.date())
    if end_date is not None:
        where.append("ld.trade_date <= ?")
        params.append(end_date.date())

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_DEAL_SELECT_COLS}, sm.company_name, sm.market_cap_cr
            FROM large_deals ld
            LEFT JOIN stock_master sm ON sm.ticker = ld.ticker
            WHERE {' AND '.join(where)}
            ORDER BY ld.trade_date DESC
            """,
            params,
        ).fetchall()

    data = [_deal_row_to_dict(r) for r in rows]
    return {"ticker": ticker, "data": data, "record_count": len(data)}


_POSITION_COLUMNS = [
    "family_id", "ticker", "trade_date", "deal_type", "net_transaction_type",
    "net_quantity", "avg_price", "exchange", "cumulative_position_est",
    "is_new_entry", "is_full_exit",
]
_POSITION_SELECT_COLS = ", ".join(f"bp.{c}" for c in _POSITION_COLUMNS)

# No is_delisted/status column exists on stock_master (create_normalised.py) —
# delisting is inferred the same way config/build_universe.py does: a ticker
# with no recent OHLCV print is treated as delisted/suspended.
_DELISTED_STALENESS_DAYS = 30


_MATERIALITY_HOLDING_PCT = 0.1  # below this, the disclosed deal isn't a meaningful stake — see get_family_entries_exits docstring


def _position_row_to_dict(row, combined_qty, wac, trendlyne_prior_holder) -> dict:
    d = dict(zip(_POSITION_COLUMNS, row[: len(_POSITION_COLUMNS)]))
    family_display_name, company_name, market_cap_cr, cmp, cmp_date = row[len(_POSITION_COLUMNS):]
    d["trade_date"] = d["trade_date"].isoformat() if d["trade_date"] else None
    d["family_display_name"] = family_display_name or d["family_id"].removeprefix("unmapped:")
    d["company_name"] = company_name
    d["market_cap_cr"] = market_cap_cr
    d["cap_band"] = _cap_band(market_cap_cr)

    # Trendlyne (public_shareholders) confirmation of a holding in a strictly
    # earlier quarter overrides the same-day bulk-deal is_new_entry flag —
    # a family can show is_new_entry=True on a given trade_date simply
    # because that's the first *trade* we've attributed to it, even though
    # Trendlyne's quarterly filings show it already held the stock.
    if trendlyne_prior_holder:
        d["entry_status"] = "old_entry"
    elif d["is_new_entry"]:
        d["entry_status"] = "new_entry"
    else:
        d["entry_status"] = None

    d["cmp"] = cmp
    d["cmp_date"] = cmp_date.isoformat() if cmp_date else None
    d["is_delisted"] = cmp_date is None or (date.today() - cmp_date).days > _DELISTED_STALENESS_DAYS
    avg_price = d["avg_price"]
    d["price_diff"] = (cmp - avg_price) if (cmp is not None and avg_price is not None) else None
    d["price_diff_pct"] = (d["price_diff"] / avg_price * 100.0) if (d["price_diff"] is not None and avg_price) else None
    d["wac"] = wac

    # Shares outstanding isn't joined directly (fundamentals.shares_outstanding
    # is PIT-gated per fiscal quarter and sparse); back-derive an estimate
    # from market_cap_cr / cmp (market_cap_cr is itself price * shares, in
    # crore) instead, since both are already on hand from this same query.
    #
    # combined_qty (from _position_and_wac_asof below) is the family's true
    # holding as of trade_date — bulk-deal rows alone (net across BULK+BLOCK
    # deal types) plus Trendlyne quarterly true-ups for trades too small to
    # be disclosed.
    shares_outstanding_est = (market_cap_cr * 1e7 / cmp) if (market_cap_cr and cmp) else None
    d["combined_cumulative_position_est"] = combined_qty
    d["holding_pct_of_company"] = (
        combined_qty / shares_outstanding_est * 100.0
        if (shares_outstanding_est and combined_qty is not None) else None
    )
    return d


_FUZZY_NAME_TOKEN_STOPWORDS = {"AND", "ASSOCIATES", "FAMILY", "THE", "MR", "MRS"}
_FUZZY_NAME_MATCH_THRESHOLD = 0.8  # token-Jaccard; see _fuzzy_match_unmapped_family


def _name_tokens(normalized_name: str) -> set:
    """normalize_client_name() output -> a set of meaningful tokens (stopwords/short tokens dropped)."""
    return {
        tok for tok in normalized_name.split(" ")
        if tok and tok not in _FUZZY_NAME_TOKEN_STOPWORDS and len(tok) > 1
    }


def _token_jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _is_positional_abbreviation_match(holder_norm: str, candidate_norm: str) -> bool:
    """
    Narrow, order-preserving fallback for the token-Jaccard check below: same
    number of space-separated tokens (including single-letter initials, which
    _name_tokens() drops), and every token pair is either identical or one is
    a same-first-letter PREFIX of the other (an initial standing in for a full
    middle/given name — "HITESH R JAVERI" vs "HITESH RAMJI JAVERI": token 2 is
    "R" vs "RAMJI", "RAMJI".startswith("R") and nothing else differs).

    Deliberately does NOT allow a bare initial to match an unrelated token
    (only a true prefix relationship counts) and requires every OTHER token to
    match exactly — so a genuinely different name ("ASHISH KACHOLIA" vs
    "ASHOK KACHOLIA": token 1 "ASHISH" vs "ASHOK" is not a prefix relation
    either direction) never passes.
    """
    a_tokens = holder_norm.split(" ")
    b_tokens = candidate_norm.split(" ")
    if len(a_tokens) != len(b_tokens) or a_tokens == b_tokens:
        return a_tokens == b_tokens
    diffs = 0
    for ta, tb in zip(a_tokens, b_tokens):
        if ta == tb:
            continue
        if not ta or not tb:
            return False
        if not (ta.startswith(tb) or tb.startswith(ta)):
            return False
        diffs += 1
    # Require at least one token to actually match exactly (an all-abbreviated
    # name with zero exact anchor tokens is too weak a signal to trust).
    return 0 < diffs < len(a_tokens)


def _fuzzy_match_unmapped_family(holder_norm: str, candidate_family_ids: list) -> Optional[str]:
    """
    BI6: find the best "unmapped:<name>" family_id (from candidate_family_ids,
    already scoped to a single ticker by the caller) whose own normalized name
    plausibly refers to the same real person/entity as holder_norm, when an
    exact normalize_client_name() re-match already failed.

    Deliberately conservative — a false-positive match here would silently
    merge two DIFFERENT real investors' bulk-deal cost bases into one
    Trendlyne cross-check, which is worse than the status quo (just missing
    the cross-check for one family/ticker). Either of two independent,
    narrowly-scoped signals is accepted:

    1. Token overlap (Jaccard over word-sets, stopwords like "AND"/
       "ASSOCIATES"/"FAMILY" excluded) >= _FUZZY_NAME_MATCH_THRESHOLD — catches
       a missing/extra "AND ASSOCIATES" suffix or reordered tokens
       ("SHAH SHARAD KANAYALAL" vs "SHARAD KANAYALAL SHAH AND ASSOCIATES")
       without matching on stopwords alone.
    2. _is_positional_abbreviation_match — same token count, order preserved,
       every token identical except one which is a same-prefix abbreviation
       ("HITESH R JAVERI" vs "HITESH RAMJI JAVERI"). Deliberately NOT a raw
       edit-distance ratio: that measure treats "ASHISH KACHOLIA" (0.80
       similarity to "ASHOK KACHOLIA", a genuinely different real investor)
       as almost as close as the true positive above, which is too risky to
       accept blindly — the positional/prefix structure check is safer.

    Only ever called with candidates already restricted to the SAME ticker
    (the caller's unmapped_by_ticker), so a coincidental cross-ticker
    name collision can't produce a false match here.

    Returns
    -------
    str or None
        The single best-matching family_id if exactly one candidate clears
        either check; None if zero or more than one candidate clears it
        (an ambiguous multi-way match is treated as no match, not a guess).
    """
    if not candidate_family_ids or not holder_norm:
        return None

    holder_tokens = _name_tokens(holder_norm)
    matches = []
    for family_id in candidate_family_ids:
        candidate_norm = family_id.removeprefix("unmapped:")
        if candidate_norm == holder_norm:
            matches.append(family_id)
            continue
        candidate_tokens = _name_tokens(candidate_norm)
        jaccard = _token_jaccard(holder_tokens, candidate_tokens)
        if jaccard >= _FUZZY_NAME_MATCH_THRESHOLD:
            matches.append(family_id)
            continue
        if _is_positional_abbreviation_match(holder_norm, candidate_norm):
            matches.append(family_id)

    if len(matches) == 1:
        return matches[0]
    return None


def _position_and_wac_asof(conn, family_tickers: list) -> dict:
    """
    Per (family_id, ticker, trade_date): the family's true cumulative
    position and weighted-average cost as of that trade_date, and whether
    Trendlyne shows them holding the stock in a strictly earlier quarter
    ("old entry" evidence).

    Built by replaying two event streams together in date order, per
    family/ticker:
      1. bulk_deal_positions rows (BULK+BLOCK combined) — a BUY adds shares
         at that day's avg_price to the cost basis; a SELL draws down
         quantity at the *existing* WAC (a sale doesn't change the cost
         basis of what's left).
      2. Trendlyne quarterly public_shareholders checkpoints — NSE only
         requires a bulk/block deal disclosure when a single trade crosses
         0.5% of the company's shares, so plenty of real purchases/sales
         never show up in bulk_deal_positions at all. Each checkpoint true
         our running quantity up to Trendlyne's reported_shares for that
         quarter:
           - if it's *lower* than what we'd tracked (a sale bulk-deal data
             never saw, partial or full), the position is trued down to
             the reported remainder and the WAC is left unchanged — the
             per-share cost basis of stock you didn't sell doesn't move.
           - if it's *higher* (an undisclosed purchase), the extra shares
             are costed at the nearest OHLCV close on/before that quarter
             (Trendlyne reports share counts, not prices, so this is an
             estimate, not the actual fill price).

    Matching a bulk-deal family_id to a Trendlyne holder is exact for
    already-seeded families (public_shareholders.family_id is set the same
    way); for "unmapped:<name>" families it's first attempted by re-normalizing
    public_shareholders.holder_name with the identical normalize_client_name
    used to build "unmapped:<name>" in bulk_deal_attribution.py, since
    Trendlyne's own family_id join comes up NULL for anyone not already in
    investor_family. [BI6, 2026-07-11] If that exact re-normalization misses,
    _fuzzy_match_unmapped_family() is tried next — a conservative token-overlap
    + edit-distance heuristic (see its docstring) restricted to the same
    ticker's other unmapped: families, so a name that differs only by
    abbreviation/punctuation/a missing "AND ASSOCIATES" suffix still
    cross-checks instead of silently losing the Trendlyne true-up.

    [Investigated, per user request] A tiny disclosed net position (e.g.
    SAKSOFT/JUNOMONETA FINSOL, ~0.001% of the company pre-materiality-
    filter) can still legitimately trigger an NSE bulk-deal disclosure:
    the 0.5% threshold is checked against the *gross* single-leg trade
    quantity, not the family's net day-over-day change in holding.
    Confirmed against large_deals: on 2026-07-03 JUNOMONETA both bought
    1,800,580 shares @181.21 (over SAKSOFT's ~765k-share 0.5% threshold)
    AND sold 1,799,016 @181.31 the same day — wash-trade netting in
    bulk_deal_attribution.py correctly nets that to a genuine +1,564 share
    change. Real, disclosure-triggering activity, just not stake-building —
    _MATERIALITY_HOLDING_PCT filters it from the response as noise, not as
    a data error.
    """
    if not family_tickers:
        return {}
    from ingestion.scrapers.bulk_deal_attribution import normalize_client_name

    where_pairs = " OR ".join(["(family_id = ? AND ticker = ?)"] * len(family_tickers))
    params: list = [v for pair in family_tickers for v in pair]
    trade_rows = conn.execute(
        f"""
        SELECT family_id, ticker, trade_date, net_transaction_type, net_quantity, avg_price
        FROM bulk_deal_positions
        WHERE {where_pairs}
        ORDER BY family_id, ticker, trade_date, deal_type
        """,
        params,
    ).fetchall()

    tickers = sorted({t for _, t in family_tickers})
    ph_rows = conn.execute(
        f"""
        SELECT family_id, holder_name, ticker, quarter_end_date, reported_shares
        FROM public_shareholders
        WHERE ticker IN ({', '.join(['?'] * len(tickers))}) AND reported_shares IS NOT NULL
        ORDER BY ticker, quarter_end_date
        """,
        tickers,
    ).fetchall()

    price_rows = conn.execute(
        f"SELECT ticker, date, close FROM ohlcv_adjusted WHERE ticker IN ({', '.join(['?'] * len(tickers))}) ORDER BY ticker, date",
        tickers,
    ).fetchall()
    price_history: dict = {}
    for ticker, d, close in price_rows:
        price_history.setdefault(ticker, []).append((d, close))

    def _nearest_close(ticker, asof):
        price = None
        for d, close in price_history.get(ticker, []):
            if d > asof:
                break
            price = close
        return price

    target_pairs = set(family_tickers)
    events: dict = {pair: [] for pair in target_pairs}
    for family_id, ticker, trade_date, net_type, net_qty, avg_price in trade_rows:
        events[(family_id, ticker)].append((trade_date, 0, ("trade", net_type, net_qty, avg_price)))

    # BI6: unmapped: families indexed per ticker, for the fuzzy fallback below.
    unmapped_by_ticker: dict = {}
    for fam_id, tick in target_pairs:
        if fam_id.startswith("unmapped:"):
            unmapped_by_ticker.setdefault(tick, []).append(fam_id)

    for family_id, holder_name, ticker, quarter_end_date, reported_shares in ph_rows:
        if family_id is not None:
            key = (family_id, ticker)
        else:
            holder_norm = normalize_client_name(holder_name)
            exact_key = (f"unmapped:{holder_norm}", ticker)
            if exact_key in events:
                key = exact_key
            else:
                # BI6: exact re-normalization missed — try a fuzzy match against
                # this ticker's other unmapped: families before giving up. See
                # _fuzzy_match_unmapped_family's docstring for the heuristic and
                # why this is deliberately conservative (false positives here
                # would silently merge two different real investors' cost bases).
                matched_family = _fuzzy_match_unmapped_family(holder_norm, unmapped_by_ticker.get(ticker, []))
                key = (matched_family, ticker) if matched_family else exact_key
        if key in events:
            events[key].append((quarter_end_date, 1, ("checkpoint", reported_shares)))

    result: dict = {}
    for (family_id, ticker), evs in events.items():
        evs.sort(key=lambda e: (e[0], e[1]))
        qty, cost = 0.0, 0.0
        trendlyne_ever_positive = False
        for event_date, _, payload in evs:
            if payload[0] == "trade":
                _, net_type, net_qty, avg_price = payload
                if net_type == "BUY":
                    qty += net_qty
                    cost += net_qty * avg_price
                else:
                    sell_qty = min(net_qty, qty) if qty > 0 else 0
                    if qty > 0:
                        cost -= sell_qty * (cost / qty)
                    qty -= sell_qty
                wac = (cost / qty) if qty > 0 else None
                result[(family_id, ticker, event_date)] = (qty, wac, trendlyne_ever_positive)
            else:
                _, reported_shares = payload
                diff = reported_shares - qty
                if diff < -0.5:
                    wac_before = (cost / qty) if qty > 0 else 0.0
                    qty = reported_shares
                    cost = wac_before * qty
                elif diff > 0.5:
                    price = _nearest_close(ticker, event_date) or ((cost / qty) if qty > 0 else None)
                    if price is not None:
                        cost += diff * price
                    qty = reported_shares
                if reported_shares > 0:
                    trendlyne_ever_positive = True
    return result


@router.get("/bulk-deals/families/entries-exits")
async def get_family_entries_exits(
    trade_date_param: Optional[date] = Query(None, alias="date", description="Optional: restrict to a single trade date (default: all history)"),
    cap_band: Optional[str] = Query(None, description="Filter: large|mid|small|micro|unknown"),
    deal_type: Optional[str] = Query(None, description="Filter: bulk|block"),
) -> dict:
    """
    Family-attributed, wash-trade-netted bulk/block deal positions across
    all of history (or a single trade_date if given), cap-band annotated.
    Reads bulk_deal_positions (Phase B), the derived table
    ingestion/scrapers/bulk_deal_attribution.py rebuilds daily from
    large_deals + investor_family.

    All entries are returned (not just one day) so a family's purchase
    price can be compared against how close the current price is to it
    across their whole trading history in a ticker. bulk_deal_positions
    currently only has 2026-07-03 loaded (see large_deals — the scraper
    hasn't backfilled/accumulated older dates yet), so today being the only
    date shown right now is a data-coverage gap, not a query bug.

    Positions the family has fully sold (cumulative_position_est <= 0),
    tickers with no recent OHLCV print (delisted/suspended proxy — see
    _DELISTED_STALENESS_DAYS), and stakes below _MATERIALITY_HOLDING_PCT of
    the company are excluded — see _position_and_wac_asof's docstring for
    why a disclosed bulk deal can still correspond to a near-0% stake
    (NSE's threshold is gross single-leg trade size, not net position
    change). New-vs-old entry status, the true cumulative position, and
    WAC are all computed together in _position_and_wac_asof by replaying
    bulk_deal_positions together with Trendlyne's quarterly
    public_shareholders filings — not every purchase/sale is large enough
    to require bulk/block deal disclosure, so Trendlyne is the only source
    that catches those. CMP and the CMP-vs-entry-price difference come
    from the latest available ohlcv_adjusted close (today's price, not the
    trade date's).

    Registered before /bulk-deals/families/{ticker} for the same
    route-ordering reason as bulk-deals/entries-exits above.
    """
    where = ["bp.cumulative_position_est > 0"]
    params: list = []
    if trade_date_param:
        where.append("bp.trade_date = ?")
        params.append(trade_date_param)
    if deal_type:
        where.append("bp.deal_type = ?")
        params.append(deal_type)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_POSITION_SELECT_COLS}, fam.family_display_name, sm.company_name, sm.market_cap_cr,
                   latest.close AS cmp, latest.date AS cmp_date
            FROM bulk_deal_positions bp
            LEFT JOIN (SELECT DISTINCT family_id, family_display_name FROM investor_family) fam
                ON fam.family_id = bp.family_id
            LEFT JOIN stock_master sm ON sm.ticker = bp.ticker
            LEFT JOIN (
                SELECT ticker, close, date FROM (
                    SELECT ticker, close, date,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                    FROM ohlcv_adjusted
                    WHERE date >= CURRENT_DATE - INTERVAL '{_DELISTED_STALENESS_DAYS * 2} days'
                ) sub WHERE rn = 1
            ) latest ON latest.ticker = bp.ticker
            WHERE {' AND '.join(where)}
            ORDER BY bp.trade_date DESC, sm.market_cap_cr ASC NULLS LAST
            """,
            params,
        ).fetchall()

        position_wac_map = _position_and_wac_asof(conn, list({(r[0], r[1]) for r in rows}))

    data = []
    for r in rows:
        family_id, ticker_val, trade_date_val = r[0], r[1], r[2]
        combined_qty, wac, trendlyne_prior_holder = position_wac_map.get(
            (family_id, ticker_val, trade_date_val), (r[8], None, False)
        )
        data.append(_position_row_to_dict(r, combined_qty, wac, trendlyne_prior_holder))

    data = [d for d in data if not d["is_delisted"]]
    data = [d for d in data if d["holding_pct_of_company"] is None or d["holding_pct_of_company"] >= _MATERIALITY_HOLDING_PCT]
    if cap_band:
        data = [d for d in data if d["cap_band"] == cap_band]
    return {
        "date": trade_date_param.isoformat() if trade_date_param else None,
        "data": data,
        "record_count": len(data),
    }


@router.get("/bulk-deals/families/{ticker}")
async def get_family_positions(
    ticker: str,
    start_date: Optional[datetime] = Query(None, description="trade_date range start (inclusive)"),
    end_date: Optional[datetime] = Query(None, description="trade_date range end (inclusive)"),
) -> dict:
    """Family-attributed position history for a single ticker, cap-band annotated."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    where = ["bp.ticker = ?"]
    params: list = [ticker]
    if start_date is not None:
        where.append("bp.trade_date >= ?")
        params.append(start_date.date())
    if end_date is not None:
        where.append("bp.trade_date <= ?")
        params.append(end_date.date())

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_POSITION_SELECT_COLS}, fam.family_display_name, sm.company_name, sm.market_cap_cr
            FROM bulk_deal_positions bp
            LEFT JOIN (SELECT DISTINCT family_id, family_display_name FROM investor_family) fam
                ON fam.family_id = bp.family_id
            LEFT JOIN stock_master sm ON sm.ticker = bp.ticker
            WHERE {' AND '.join(where)}
            ORDER BY bp.trade_date DESC
            """,
            params,
        ).fetchall()

    data = [_position_row_to_dict(r, r[8], None, False) for r in rows]
    return {"ticker": ticker, "data": data, "record_count": len(data)}


def _latest_two_months(conn, as_of: date) -> list:
    rows = conn.execute(
        "SELECT DISTINCT month FROM mf_holdings WHERE availability_date <= ? ORDER BY month DESC LIMIT 2",
        [as_of],
    ).fetchall()
    return [r[0] for r in rows]


def _mf_movers_rows(conn, as_of: date) -> list:
    """
    Per-ticker MF-holding movers between the two most recent PIT-available
    months. Returns dicts with ticker, company_name, market_cap_cr,
    cap_band, curr_month, prev_month, curr_qty, prev_qty, qty_change,
    qty_change_pct, curr_scheme_count, direction ('new_entry' | 'full_exit'
    | 'increasing' | 'decreasing' | 'unchanged').
    """
    months = _latest_two_months(conn, as_of)
    if not months:
        return []
    curr_month = months[0]
    prev_month = months[1] if len(months) > 1 else None

    rows = conn.execute(
        """
        WITH curr AS (
            SELECT ticker, SUM(quantity) AS qty, SUM(value_inr) AS value_inr, COUNT(DISTINCT scheme_name) AS scheme_count
            FROM mf_holdings WHERE month = ? GROUP BY ticker
        ),
        prev AS (
            SELECT ticker, SUM(quantity) AS qty, COUNT(DISTINCT scheme_name) AS scheme_count
            FROM mf_holdings WHERE month = ? GROUP BY ticker
        )
        SELECT
            COALESCE(curr.ticker, prev.ticker) AS ticker,
            curr.qty AS curr_qty, prev.qty AS prev_qty,
            curr.value_inr AS curr_value_inr,
            curr.scheme_count AS curr_scheme_count, prev.scheme_count AS prev_scheme_count,
            sm.company_name, sm.market_cap_cr
        FROM curr
        FULL OUTER JOIN prev ON prev.ticker = curr.ticker
        LEFT JOIN stock_master sm ON sm.ticker = COALESCE(curr.ticker, prev.ticker)
        """,
        [curr_month, prev_month],
    ).fetchall()

    data = []
    for (
        ticker, curr_qty, prev_qty, curr_value_inr, curr_scheme_count, prev_scheme_count,
        company_name, market_cap_cr,
    ) in rows:
        curr_qty = curr_qty or 0
        prev_qty = prev_qty or 0
        curr_scheme_count = curr_scheme_count or 0
        prev_scheme_count = prev_scheme_count or 0
        qty_change = curr_qty - prev_qty
        qty_change_pct = (qty_change / prev_qty * 100.0) if prev_qty else None
        scheme_count_change = curr_scheme_count - prev_scheme_count
        if prev_qty == 0 and curr_qty > 0:
            direction = "new_entry"
        elif prev_qty > 0 and curr_qty == 0:
            direction = "full_exit"
        elif qty_change > 0:
            direction = "increasing"
        elif qty_change < 0:
            direction = "decreasing"
        else:
            direction = "unchanged"
        data.append({
            "ticker": ticker,
            "company_name": company_name,
            "market_cap_cr": market_cap_cr,
            "cap_band": _cap_band(market_cap_cr),
            "curr_month": curr_month.isoformat() if curr_month else None,
            "prev_month": prev_month.isoformat() if prev_month else None,
            "curr_qty": int(curr_qty),
            "prev_qty": int(prev_qty),
            "qty_change": int(qty_change),
            "qty_change_pct": qty_change_pct,
            "curr_value_inr": curr_value_inr,
            "curr_scheme_count": curr_scheme_count,
            "prev_scheme_count": prev_scheme_count,
            "scheme_count_change": scheme_count_change,
            "direction": direction,
        })
    return data


@router.get("/mf-holdings/movers")
async def get_mf_holdings_movers(
    direction: Optional[str] = Query(None, description="Filter: new_entry|full_exit|increasing|decreasing"),
    cap_band: Optional[str] = Query(None, description="Filter: large|mid|small|micro|unknown"),
    as_of: Optional[datetime] = Query(None, description="PIT reference (default: today)"),
) -> dict:
    """
    MF-holding movers (increasing/decreasing/new-entry/full-exit) between
    the two most recent PIT-available months, cap-band annotated.

    Registered before /mf-holdings/{ticker} for the same route-ordering
    reason as the bulk-deals endpoints above.
    """
    pit_reference = (as_of.date() if as_of else datetime.utcnow().date())
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        data = _mf_movers_rows(conn, pit_reference)

    if direction:
        data = [d for d in data if d["direction"] == direction]
    if cap_band:
        data = [d for d in data if d["cap_band"] == cap_band]
    data.sort(key=lambda d: d["market_cap_cr"] if d["market_cap_cr"] is not None else float("inf"))
    return {"as_of": pit_reference.isoformat(), "data": data, "record_count": len(data)}


@router.get("/mf-holdings/{ticker}")
async def get_mf_holdings_for_ticker(
    ticker: str,
    start_month: Optional[str] = Query(None, description="'YYYY-MM' range start (inclusive)"),
    end_month: Optional[str] = Query(None, description="'YYYY-MM' range end (inclusive)"),
) -> dict:
    """Per-scheme MF holdings history for a single ticker."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")

    where = ["ticker = ?"]
    params: list = [ticker]
    if start_month:
        where.append("month >= ?")
        params.append(f"{start_month}-01")
    if end_month:
        where.append("month <= ?")
        params.append(f"{end_month}-01")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT ticker, month, scheme_name, isin, quantity, value_inr, availability_date
            FROM mf_holdings WHERE {' AND '.join(where)}
            ORDER BY month DESC, scheme_name
            """,
            params,
        ).fetchall()

    data = [
        {
            "ticker": r[0], "month": r[1].isoformat(), "scheme_name": r[2], "isin": r[3],
            "quantity": r[4], "value_inr": r[5], "availability_date": r[6].isoformat(),
        }
        for r in rows
    ]
    return {"ticker": ticker, "data": data, "record_count": len(data)}


_RECONCILIATION_COLUMNS = [
    "id", "family_id", "ticker", "quarter_end_date", "filing_date",
    "estimated_position_pre_correction", "reported_shares_est", "correction_applied",
    "correction_delta", "discrepancy_pct", "status", "reviewed_by", "reviewed_at", "notes",
]
_RECONCILIATION_SELECT_COLS = ", ".join(_RECONCILIATION_COLUMNS)


class ReconciliationResolve(BaseModel):
    reviewed_by: str
    notes: Optional[str] = None


@router.get("/reconciliation")
async def get_reconciliation_log(
    status: Optional[str] = Query(None, description="Filter: resolved|flagged_for_review"),
    family_id: Optional[str] = Query(None),
) -> dict:
    """Bulk-deal-vs-public-shareholders reconciliation log (Phase D)."""
    where = ["1=1"]
    params: list = []
    if status:
        where.append("status = ?")
        params.append(status)
    if family_id:
        where.append("family_id = ?")
        params.append(family_id)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_RECONCILIATION_SELECT_COLS} FROM bulk_deal_reconciliation_log
            WHERE {' AND '.join(where)}
            ORDER BY quarter_end_date DESC, id DESC
            """,
            params,
        ).fetchall()

    data = []
    for r in rows:
        d = dict(zip(_RECONCILIATION_COLUMNS, r))
        d["quarter_end_date"] = d["quarter_end_date"].isoformat() if d["quarter_end_date"] else None
        d["filing_date"] = d["filing_date"].isoformat() if d["filing_date"] else None
        d["reviewed_at"] = d["reviewed_at"].isoformat() if d["reviewed_at"] else None
        data.append(d)
    return {"data": data, "record_count": len(data)}


@router.post("/reconciliation/{log_id}/resolve")
async def resolve_reconciliation(log_id: int, body: ReconciliationResolve) -> dict:
    """Mark a flagged reconciliation row as reviewed (does not re-touch bulk_deal_positions)."""
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        existing = conn.execute(
            "SELECT id FROM bulk_deal_reconciliation_log WHERE id = ?", [log_id]
        ).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"No reconciliation log row with id={log_id}")

        conn.execute(
            """
            UPDATE bulk_deal_reconciliation_log
            SET status = 'resolved', reviewed_by = ?, reviewed_at = ?, notes = ?
            WHERE id = ?
            """,
            [body.reviewed_by, datetime.utcnow(), body.notes, log_id],
        )
    return {"id": log_id, "status": "resolved", "reviewed_by": body.reviewed_by}

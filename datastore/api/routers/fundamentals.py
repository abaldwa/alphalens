"""
datastore/api/routers/fundamentals.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-PIPE-003 (CRITICAL)
Owner: Platform / DataStore
Consumers: ingestion/scrapers/screener.py, features/fundamental.py

GET /api/v1/fundamentals/{ticker}?start_date=&end_date=&as_of= and
POST /api/v1/fundamentals/write — against the `fundamentals` DuckDB table
(Store 2, datastore/schema/create_normalised.py). One row per
(ticker, fiscal_year, quarter).

[AS BUILT] Replaces main.py's P0.1 stub `GET /api/v1/fundamentals/{ticker}`
(which always returned an empty list — never wired to a real query) — same
"move inline stub into a real router" pattern as every other P1.7 router.

SPEC-PIPE-003 (CRITICAL): the GET here enforces PIT correctness via
datastore/api/pit.py's enforce_pit_fundamentals — only rows with
announcement_date <= as_of are returned, sorted ascending by
announcement_date. quarter_end_date is never used as a filter or sort key.

[AS BUILT, SPEC-SCHED-013] persist=False on every connection — DUCKDB_PATH
is also written by the ingestion scheduler from a separate long-lived
process; see ohlcv.py's module docstring for the full incident this avoids.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from config.settings import DUCKDB_PATH
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from datastore.api.pit import enforce_pit_fundamentals
from datastore.api.schemas import (
    FAPeerRow,
    FAPeersResponse,
    FARatiosResponse,
    FAScoresResponse,
    FAScreenerResponse,
    FASectorResponse,
    FundamentalsResponse,
    FundamentalsRow,
    FundamentalsWrite,
    FundamentalsWriteBatch,
    FundamentalsWriteBatchResult,
    FundamentalsWriteResult,
)
from datastore.api.utils.feature_store import read_feature_day, read_feature_row, resolve_date
from datastore.api.utils.pdf import build_pdf_response
from features.fundamental import RATIO_FEATURES, STALENESS_FEATURES
from features.fundamental_composites import (
    SCREENER_PRESETS,
    growth_score,
    management_quality_score,
    matches_screener_preset,
    quality_score,
    select_peers,
)
from features.fundamental_quality_gate import validate_and_annotate
from features.fundamental_source_priority import SOURCE_PRIORITY, build_priority_update_clause
from features.governance import GOVERNANCE_FEATURES

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/fundamentals", tags=["Fundamentals"])

_COLUMNS = [
    "ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date",
    "revenue", "ebitda", "ebit", "pat", "eps", "operating_margin", "ebitda_margin", "net_margin",
    "roe", "roce", "debt_to_equity", "interest_coverage", "fcf", "asset_turnover",
    "inventory_days", "receivable_days", "payable_days", "book_value_per_share", "shares_outstanding",
    "gross_profit", "capex", "current_assets", "current_liabilities", "total_debt", "cash_and_equivalents",
    "depreciation",
    "sector_specific_metric_1", "sector_specific_metric_2", "sector_specific_metric_3",
    "sector_specific_metric_4", "sector_specific_metric_5", "sector_specific_metric_6",
    # 2026-07-07: these exist in the DB (P3.11 + this session's deep-forensic
    # gap fix) but were missing from this SELECT list, so every GET response
    # silently omitted them even after the Pydantic schema was fixed — see
    # schemas.py's FundamentalsWrite docstring note for the full incident.
    "total_equity", "retained_earnings", "total_assets", "cwip",
    # 2026-07-07: NSE XBRL Integrated Filing pipeline — see
    # ingestion/scrapers/nse_xbrl_financials.py and datastore/schema/
    # create_normalised.py's _CREATE_FUNDAMENTALS comment for sourcing.
    "goodwill", "inventories", "trade_receivables_current", "trade_payables_current",
    "total_liabilities", "audit_qualified_flag",
    "property_plant_equipment", "intangible_assets", "non_current_investments",
    "non_current_trade_receivables", "deferred_tax_assets", "current_investments",
    "current_tax_assets", "borrowings_current", "borrowings_noncurrent",
    "deferred_tax_liabilities", "provisions_current", "provisions_noncurrent",
    "equity_share_capital", "other_equity", "non_controlling_interest", "non_current_liabilities",
    # [AS BUILT, A36 fix 2026-07-09] quality_flag/quality_flag_reason (see
    # features/fundamental_quality_gate.py) and provenance (see
    # features/fundamental_source_priority.py) were previously never
    # written by this endpoint — screener.py bypassed A12's range-
    # validation gate entirely, unlike trendlyne/kaggle. Both now wired in
    # below.
    "quality_flag", "quality_flag_reason",
    "fundamentals_source", "fundamentals_source_priority",
]
_SELECT_COLS = ", ".join(_COLUMNS)


# [AS BUILT, P2.6] MUST be registered before /{ticker}: FastAPI/Starlette
# matches routes by registration order, and the dynamic /{ticker} pattern
# would otherwise swallow "RELIANCE/history" as ticker="RELIANCE",
# {missing path segment} — same route-ordering discipline as signals.py's
# documented /ml/top_buys/{date}-before-/ml/{ticker}/{date} fix.
@router.get("/{ticker}/history", response_model=FundamentalsResponse)
async def get_fundamentals_history_by_quarters(
    ticker: str,
    quarters: int = Query(8, ge=1, le=80, description="Number of most recent quarters to return"),
    as_of: Optional[datetime] = Query(None, description="PIT reference (default: now)"),
) -> FundamentalsResponse:
    """
    Most recent `quarters` quarterly fundamentals rows for a ticker,
    PIT-filtered by announcement_date <= as_of, descending by
    quarter_end_date trimmed to `quarters` rows then re-sorted ascending
    by announcement_date (same ordering convention as GET /{ticker}).

    [AS BUILT, P2.6] Distinct from datastore/client.py's existing
    get_fundamentals_history(ticker, as_of, lookback_years) — that method
    calls GET /{ticker} with a YEAR-based lookback window; this build
    prompt's literal `?quarters=8` is a COUNT-based window instead (some
    tickers report irregularly / have gaps, where N years doesn't reliably
    mean N*4 quarters). Both are kept — neither supersedes the other.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    pit_reference = as_of or datetime.utcnow()

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM fundamentals
            WHERE ticker = ? AND announcement_date <= ?
            ORDER BY quarter_end_date DESC
            LIMIT ?
            """,
            [ticker, pit_reference.date(), quarters],
        ).fetchall()

    df = pd.DataFrame(rows, columns=_COLUMNS)
    if not df.empty:
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
        # 2026-07-07: same tie-break fix as GET /{ticker} — see that route's
        # comment for the full incident (a Screener row and an NSE-XBRL row
        # for the same real quarter, identical quarter_end_date AND
        # approximated announcement_date, different (fiscal_year, quarter)
        # labels). This route additionally returns multiple quarters (not
        # just the single latest), so the tiebreak matters for every
        # duplicate-quarter pair in the window, not just the newest.
        df["_nonnull_count"] = df.notna().sum(axis=1)
        df = df.sort_values(["announcement_date", "quarter_end_date", "_nonnull_count"]).drop(columns="_nonnull_count")
        df = df.astype(object).where(df.notna(), None)

    data = [FundamentalsRow(**row) for row in df.to_dict(orient="records")]
    return FundamentalsResponse(ticker=ticker, as_of=pit_reference, data=data, record_count=len(data))


# ===== SPEC-FA-008: Fundamental Analysis API scaffolding over the already-
# computed sector-relative z-scored ratios (features/fundamental.py) and
# governance features (features/governance.py), both already merged into
# the daily feature Parquet by features/matrix_builder.py — see
# datastore/api/utils/feature_store.py. Registered before the bare
# /{ticker} and /screener before any dynamic single-segment route, for the
# same route-ordering reason /{ticker}/history is registered above
# /{ticker} (this file's own earlier comment; FastAPI matches by
# registration order, not specificity). =====
@router.get("/screener", response_model=FAScreenerResponse)
async def get_fundamental_screener(
    preset: str = Query(..., description=f"One of: {', '.join(SCREENER_PRESETS.keys())}"),
) -> FAScreenerResponse:
    """Tickers matching a named screener preset, evaluated against the
    latest day's sector-relative z-scored ratios — "quality compounder"
    etc. mean above/below sector peers, not an absolute % threshold (the
    feature store only carries z-scores, see features/fundamental.py)."""
    if preset not in SCREENER_PRESETS:
        raise HTTPException(status_code=400, detail=f"Unknown preset '{preset}'. Valid: {list(SCREENER_PRESETS.keys())}")

    resolved_date = resolve_date(None)
    if resolved_date is None:
        return FAScreenerResponse(preset=preset)

    panel = read_feature_day(resolved_date)
    if panel is None:
        return FAScreenerResponse(preset=preset, date=resolved_date)

    matched = [
        row["ticker"] for _, row in panel.iterrows()
        if matches_screener_preset({c: row.get(c) for c in RATIO_FEATURES}, preset)
    ]
    # ML24 (2026-07-11): ranked/recommended screener output only — direct
    # ticker lookups (get_fundamental_ratios above) are intentionally
    # unaffected, per product decision (fundamentals aren't liquidity-
    # dependent, users can still research any stock directly).
    from config.training_universe import filter_recommendable

    matched_df = filter_recommendable(pd.DataFrame({"ticker": matched}))
    matched = matched_df["ticker"].tolist()
    return FAScreenerResponse(preset=preset, date=resolved_date, tickers=matched)


@router.get("/sector/{sector}", response_model=FASectorResponse)
async def get_fundamental_sector(sector: str) -> FASectorResponse:
    """Sector aggregate of the standard ratio set (real, computed by
    averaging the day's sector-relative z-scores for tickers in this
    sector). Sector-*unique* metrics (GNPA for banks, ANDA for pharma —
    the sector_specific_metric_1-6 columns) are never actually computed
    anywhere in this codebase, so they're not included — see this
    response's `note` field."""
    resolved_date = resolve_date(None)
    if resolved_date is None:
        return FASectorResponse(sector=sector)

    panel = read_feature_day(resolved_date)
    if panel is None:
        return FASectorResponse(sector=sector, date=resolved_date)

    universe = load_universe_raw()
    sector_map = dict(zip(universe["ticker"], universe["sector"]))
    panel = panel.copy()
    panel["sector"] = panel["ticker"].map(lambda t: sector_map.get(t, "UNKNOWN"))
    sector_rows = panel[panel["sector"] == sector]
    if sector_rows.empty:
        return FASectorResponse(sector=sector, date=resolved_date, ticker_count=0)

    avg_ratios = {c: (None if pd.isna(sector_rows[c]).all() else float(sector_rows[c].mean())) for c in RATIO_FEATURES if c in sector_rows.columns}
    return FASectorResponse(sector=sector, date=resolved_date, ticker_count=len(sector_rows), avg_ratios=avg_ratios)


@router.get("/{ticker}/ratios", response_model=FARatiosResponse)
async def get_fundamental_ratios(ticker: str) -> FARatiosResponse:
    """The 27 sector-relative z-scored ratios + 3 staleness flags for one
    ticker, already computed and sitting in the daily feature Parquet."""
    resolved_date = resolve_date(None)
    if resolved_date is None:
        return FARatiosResponse(ticker=ticker)

    row = read_feature_row(ticker, resolved_date)
    if row is None:
        return FARatiosResponse(ticker=ticker, date=resolved_date)

    cols = RATIO_FEATURES + STALENESS_FEATURES
    ratios = {c: (None if c not in row or pd.isna(row[c]) else float(row[c])) for c in cols}
    return FARatiosResponse(ticker=ticker, date=resolved_date, available=True, ratios=ratios)


@router.get("/{ticker}/peers", response_model=FAPeersResponse)
async def get_fundamental_peers(
    ticker: str, k: int = Query(5, ge=1, le=20, description="Number of peers")
) -> FAPeersResponse:
    """Real peer-selection (was an unimplemented systems/fundamental_analysis/
    peers/ stub) — same sector, ranked by market-cap proximity, then each
    peer's already-computed sector-relative ratios."""
    resolved_date = resolve_date(None)
    if resolved_date is None:
        return FAPeersResponse(ticker=ticker)

    panel = read_feature_day(resolved_date)
    if panel is None:
        return FAPeersResponse(ticker=ticker, date=resolved_date)

    universe = load_universe_raw()
    sector_map = dict(zip(universe["ticker"], universe["sector"]))
    mcap_map = dict(zip(universe["ticker"], universe["market_cap_cr"]))
    peer_tickers = select_peers(ticker, panel, sector_map, mcap_map, k=k)

    peer_rows = []
    for t in peer_tickers:
        prow = panel[panel["ticker"] == t]
        if prow.empty:
            continue
        r = prow.iloc[0]
        peer_rows.append(FAPeerRow(
            ticker=t,
            roe=None if pd.isna(r.get("roe")) else float(r["roe"]),
            roce=None if pd.isna(r.get("roce")) else float(r["roce"]),
            debt_to_equity=None if pd.isna(r.get("debt_to_equity")) else float(r["debt_to_equity"]),
            pe_ratio=None if pd.isna(r.get("pe_ratio")) else float(r["pe_ratio"]),
        ))

    return FAPeersResponse(ticker=ticker, date=resolved_date, sector=sector_map.get(ticker), peers=peer_rows)


@router.get("/{ticker}/scores", response_model=FAScoresResponse)
async def get_fundamental_scores(ticker: str) -> FAScoresResponse:
    """Quality/growth/management-quality composite scores
    (features/fundamental_composites.py) — net-new small functions over
    already-computed ratio/governance values, see that module's docstring."""
    resolved_date = resolve_date(None)
    if resolved_date is None:
        return FAScoresResponse(ticker=ticker)

    row = read_feature_row(ticker, resolved_date)
    if row is None:
        return FAScoresResponse(ticker=ticker, date=resolved_date)

    ratios = {c: row.get(c) for c in RATIO_FEATURES if c in row.index}
    governance = {c: row.get(c) for c in GOVERNANCE_FEATURES if c in row.index}

    return FAScoresResponse(
        ticker=ticker,
        date=resolved_date,
        quality_score=quality_score(ratios),
        growth_score=growth_score(ratios),
        management_quality_score=management_quality_score(governance),
    )


# F4 — mirrors dashboard/static/fundamental/js/thesis.js's RATIO_LABELS /
# LOWER_IS_BETTER exactly, so the PDF's Strengths/Risks match the on-screen
# Thesis Builder sentence-for-sentence (same real z-score threshold, no
# generative text either place).
_THESIS_RATIO_LABELS = {
    "roe": "ROE", "roce": "ROCE", "net_margin": "Net margin",
    "revenue_growth_yoy": "Revenue growth (YoY)", "eps_growth_yoy": "EPS growth (YoY)",
    "debt_to_equity": "Debt/Equity",
}
_THESIS_LOWER_IS_BETTER = {"debt_to_equity", "pe_ratio"}


@router.get("/{ticker}/thesis/pdf")
async def get_fundamental_thesis_pdf(ticker: str):
    """
    F4 — server-side PDF export of the Thesis Builder screen: same real
    Strengths/Risks sentences as thesis.js (real sector-relative z-scores
    crossing the +/-0.5 threshold, no generative text), rendered as an
    actual PDF document via reportlab rather than a screenshot/print.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    ticker = ticker.upper()

    resolved_date = resolve_date(None)
    if resolved_date is None:
        raise HTTPException(status_code=404, detail="No feature data available at all")
    row = read_feature_row(ticker, resolved_date)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No ratio data for {ticker} yet")

    ratios = {c: (None if c not in row or pd.isna(row[c]) else float(row[c])) for c in RATIO_FEATURES}
    quality = quality_score(ratios)
    growth = growth_score(ratios)

    strengths, risks = [], []
    for key, label in _THESIS_RATIO_LABELS.items():
        raw = ratios.get(key)
        if raw is None:
            continue
        z = -raw if key in _THESIS_LOWER_IS_BETTER else raw
        if z > 0.5:
            strengths.append(f"{label} is {z:.1f} sector-std above peers")
        elif z < -0.5:
            risks.append(f"{label} is {abs(z):.1f} sector-std below peers")

    subtitle = (
        f"Quality {quality:.0f}" if quality is not None else "Quality —"
    ) + " | " + (f"Growth {growth:.0f}" if growth is not None else "Growth —") + f" | as of {resolved_date}"

    return build_pdf_response(
        filename=f"{ticker}_thesis.pdf",
        title=f"Investment Thesis — {ticker}",
        subtitle=subtitle,
        sections=[("Strengths", strengths), ("Risks", risks)],
    )


@router.get("/{ticker}", response_model=FundamentalsResponse)
async def get_fundamentals(
    ticker: str,
    start_date: datetime = Query(..., description="quarter_end_date range start (inclusive)"),
    end_date: datetime = Query(..., description="quarter_end_date range end (inclusive)"),
    as_of: Optional[datetime] = Query(
        None, description="PIT reference (default: end_date); only rows with announcement_date <= as_of are returned"
    ),
) -> FundamentalsResponse:
    """
    Query fundamentals for a ticker, PIT-filtered by announcement_date.

    SPEC-PIPE-003 (CRITICAL): start_date/end_date bound the
    quarter_end_date fetch window (which quarters to consider at all);
    as_of is the actual PIT gate — a quarter whose announcement_date is
    after as_of is excluded even if its quarter_end_date falls inside the
    window, since that result was not yet public knowledge as of as_of.
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    pit_reference = as_of or end_date

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM fundamentals
            WHERE ticker = ? AND quarter_end_date >= ? AND quarter_end_date <= ?
            """,
            [ticker, start_date.date(), end_date.date()],
        ).fetchall()

    df = pd.DataFrame(rows, columns=_COLUMNS)
    if not df.empty:
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
        df = enforce_pit_fundamentals(df, as_of=pit_reference, announcement_date_col="announcement_date")
        # 2026-07-07: real tie-break bug caught via NSE XBRL pipeline
        # verification — a Screener-sourced row and an NSE-XBRL-sourced row
        # for the SAME real quarter (confirmed: identical quarter_end_date)
        # can carry DIFFERENT (fiscal_year, quarter) labels — Screener's
        # _indian_fiscal_year_quarter mislabeled a 2026-03-31 quarter as
        # (2025, 1) instead of the documented-convention-correct (2026, 4)
        # this pipeline computed for the same date — and both then get the
        # same approximated announcement_date (neither source gives a true
        # filing timestamp), so enforce_pit_fundamentals' single-key sort
        # can't disambiguate at all. DataStoreClient.get_fundamentals_pit's
        # `rows[-1]` was silently picking whichever row DuckDB happened to
        # return first, sometimes the older/less-complete one. Break a full
        # tie by preferring the row with fewer NULL columns (more complete
        # data) — a generic, source-agnostic tiebreaker; does not attempt to
        # fix Screener's underlying (fiscal_year, quarter) mislabeling bug
        # itself, which is out of scope here.
        df["_nonnull_count"] = df.notna().sum(axis=1)
        df = df.sort_values(["announcement_date", "quarter_end_date", "_nonnull_count"]).drop(columns="_nonnull_count")
    # NaN → None: cast to object dtype first so pandas doesn't coerce None back to NaN
    # in float64 columns. Pydantic v2 rejects float('nan') for finite-number fields.
    if not df.empty:
        df = df.astype(object).where(df.notna(), None)

    data = [FundamentalsRow(**row) for row in df.to_dict(orient="records")]
    return FundamentalsResponse(ticker=ticker, as_of=pit_reference, data=data, record_count=len(data))


_NON_DATA_COLS = ("ticker", "fiscal_year", "quarter", "fundamentals_source", "fundamentals_source_priority")
_UPDATE_COLS = [c for c in _COLUMNS if c not in _NON_DATA_COLS]
# [AS BUILT, A36 fix 2026-07-09] Shared priority-aware merge clause
# (features/fundamental_source_priority.py) — replaces the hand-written
# `COALESCE(excluded.col, fundamentals.col)` (new-value-wins-when-both-
# present) this endpoint used through P2.6. Still additive (a NULL in
# the incoming payload never blanks an existing value — same tijori.py/
# screener.py two-writer contract the old comment documented), but a
# REAL conflict (both sides non-NULL) is now resolved by
# nse_xbrl > trendlyne > screener > kaggle priority, not "whichever
# source's write happened to run last" — see A36 in FeatureBacklog.md.
_UPDATE_CLAUSE = build_priority_update_clause(_UPDATE_COLS)
_INSERT_SQL = f"""
    INSERT INTO fundamentals ({_SELECT_COLS}) VALUES ({", ".join("?" for _ in _COLUMNS)})
    ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET {_UPDATE_CLAUSE}
"""


def _validate_and_check_pit(record: FundamentalsWrite) -> None:
    if record.announcement_date.date() <= record.quarter_end_date.date():
        raise HTTPException(
            status_code=400,
            detail=f"SPEC-PIPE-003 violation for {record.ticker} FY{record.fiscal_year}Q{record.quarter}: "
                   "announcement_date must be after quarter_end_date",
        )


def _build_fundamentals_row(record: FundamentalsWrite, source: str) -> list:
    """
    Shared row-builder for both /write and /write_batch — SPEC-PIPE-003
    check + A12/A36's range-validation gate + provenance stamping, in one
    place so the two endpoints can't drift (the exact class of bug A36
    itself found across the 4 backfill scripts).
    """
    # [AS BUILT, A36 fix 2026-07-09] Run through the same range-validation
    # gate trendlyne/kaggle already used (features/fundamental_quality_gate.py)
    # — this endpoint previously bypassed it entirely, one of the two A36
    # findings.
    write_cols = [c for c in _COLUMNS if c not in
                  ("ticker", "fiscal_year", "quarter", "quarter_end_date", "announcement_date",
                   "fundamentals_source", "fundamentals_source_priority")]
    payload = {c: getattr(record, c) for c in write_cols if hasattr(record, c)}
    payload.update({
        "ticker": record.ticker,
        "fiscal_year": record.fiscal_year,
        "quarter": record.quarter,
    })
    annotated = validate_and_annotate(payload)

    row = dict(annotated)
    row["quarter_end_date"] = record.quarter_end_date.date()
    row["announcement_date"] = record.announcement_date.date()
    row["fundamentals_source"] = source
    row["fundamentals_source_priority"] = SOURCE_PRIORITY[source]
    return [row[col] for col in _COLUMNS]


@router.post("/write", response_model=FundamentalsWriteResult)
async def write_fundamentals(record: FundamentalsWrite) -> FundamentalsWriteResult:
    """
    Upsert one quarterly fundamentals row — SPEC-DS-004:
    same (ticker, fiscal_year, quarter) replaces, never duplicates.

    Raises
    ------
    HTTPException 400
        If announcement_date <= quarter_end_date (SPEC-PIPE-003: a build
        failure — results cannot be announced before the quarter they
        cover has even ended).
    """
    _validate_and_check_pit(record)
    values = _build_fundamentals_row(record, "screener")

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        conn.execute(_INSERT_SQL, values)

    logger.info(f"fundamentals.write: {record.ticker} FY{record.fiscal_year}Q{record.quarter}")
    return FundamentalsWriteResult(
        ticker=record.ticker, fiscal_year=record.fiscal_year, quarter=record.quarter, written=True
    )


@router.post("/write_batch", response_model=FundamentalsWriteBatchResult)
async def write_fundamentals_batch(body: FundamentalsWriteBatch) -> FundamentalsWriteBatchResult:
    """
    [AS BUILT, A35 fix 2026-07-09] Upsert many quarterly fundamentals rows
    in ONE request/ONE DuckDB write-lock acquisition — closes the A35 gap
    (screener's per-ticker HTTP POST design couldn't join A25's
    staged/batch-publish pattern the way the other 4 fundamentals sources
    did). See ingestion/scrapers/screener.py::batch_export, which
    accumulates records in memory across a chunk of tickers and calls this
    once per chunk instead of once per ticker.

    One bad row (e.g. a SPEC-PIPE-003 violation) is isolated and counted
    in `failed`, never aborting the rest of the batch — same per-ticker
    isolation batch_export already had, now at row-validation granularity
    instead of at the HTTP-call granularity.
    """
    all_values = []
    failed = 0
    for record in body.records:
        try:
            _validate_and_check_pit(record)
            all_values.append(_build_fundamentals_row(record, "screener"))
        except HTTPException as exc:
            logger.warning(f"fundamentals.write_batch: skipping {record.ticker} — {exc.detail}")
            failed += 1

    if all_values:
        with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
            conn.executemany(_INSERT_SQL, all_values)

    logger.info(f"fundamentals.write_batch: {len(all_values)} written, {failed} failed of {len(body.records)}")
    return FundamentalsWriteBatchResult(written=len(all_values), failed=failed)

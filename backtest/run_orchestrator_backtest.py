"""
backtest/run_orchestrator_backtest.py

Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m backtest.run_orchestrator_backtest`),
datastore/api/routers/backtest_runs.py (trigger endpoint, background)

CLI driver for backtest/core/engine.py's BacktestOrchestrator — the
shared, channel-agnostic orchestrator every Technical/Fundamental/
Momentum adapter plugs into (backtest/adapters/*.py). Unlike
run_phase1/2/3_backtest.py (which drive the OLDER, ML-specific
BacktestEngine directly), this script exists to give the newer
orchestrator a real-data entry point of its own — until now every real
invocation was a test (test_momentum_adapter.py, test_core_engine.py);
there was no way to run it against real data from the command line or
the UI.

Real OHLCV/universe/sector data is fetched via DataStoreClient (SPEC-DS-
002 — reuses run_phase1_backtest.py's fetch helpers rather than
duplicating them) and turned into the plain closures/dicts
OrchestratorConfig expects (price_lookup, universe_provider,
sector_lookup). universe_provider is built PIT-safe from that same real
OHLCV (_build_config's ticker_dates/universe_provider below): a ticker is
only offered as a candidate on dates where it actually has a recent real
trading bar, so a stock that lists mid-window or delists mid-window is
correctly absent as a candidate before it existed / after it stopped
trading — no fabricated listing/delisting dates, and no separate data
source needed since it's derived entirely from OHLCV already in hand.
This does NOT yet consult the real `delisted_companies`/`stock_master.
listing_date` tables (a belt-and-suspenders improvement flagged as
follow-up work, since delisted_companies' own scraper is unverified —
see datastore/schema/create_normalised.py's _CREATE_DELISTED_COMPANIES) —
OHLCV presence alone already closes the sharpest form of the bias (a
ticker that doesn't exist yet showing up as a candidate on day one).

The result is persisted via backtest.core.run_store.save_run_result()
into the SAME backtest_runs table every other channel/run already writes
to — so a run triggered here shows up in the existing GET /api/v1/
backtest/runs listing (and the Backtest page's Runs table) with no
separate read path needed.
"""

import argparse
import contextlib
import hashlib
import json
import logging
import time
import uuid
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from backtest.adapters.fundamental_adapter import BESPOKE_PRESETS, FundamentalAdapter
from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.adapters.technical_adapter import TechnicalAdapter
from backtest.batch_common import exclusive_backtest_lock
from backtest.core.engine import EXIT_POLICY_VARIANTS, BacktestOrchestrator, OrchestratorConfig, build_exit_model_for_variant
from backtest.core.feature_log import FeatureLogWriter
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun
from backtest.core.run_store import save_run_result
from backtest.run_phase1_backtest import _real_sector_map
from backtest.strategy_id import (
    CODE_TO_HORIZON,
    build_strategy_id,
    default_horizon_for_fundamental,
    default_horizon_for_momentum,
    default_horizon_for_technical,
)
from config.settings import (
    BACKTEST_DUCKDB_PATH, DUCKDB_PATH,
    DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS, DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S, DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
)
from config.timezone import now_ist
from config.universe import get_tickers
from datastore.api.db import get_duckdb_connection
from backtest.export_trade_book import export_trade_book
from datastore.client import DataStoreClient
from datastore.schema.create_backtest import create_backtest_schema
from datastore.schema.create_strategy_catalog import create_strategy_catalog_schema
from systems.technical_analysis.screener.templates import TEMPLATE_STYLE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

HORIZON_BUCKET_MAP = {b.value: b for b in HorizonBucket}


def _fetch_real_ohlcv(max_tickers: Optional[int], min_history_days: int, start_date: date_type, end_date: date_type) -> pd.DataFrame:
    """Real OHLCV (config.universe.get_tickers()'s curated universe) over
    exactly [start_date, end_date] — the run's own requested window, not
    run_phase1_backtest.py's fixed 5-year lookback.

    2026-07-26 fix (backtest-reviewer sign-off): was a per-ticker
    GET /ohlcv/{ticker} loop — measured at ~7-8s/ticker under API load, so a
    ~2300-ticker universe took hours just to fetch before any backtest
    compute started (FeatureBacklog A73). Switched to one
    GET /ohlcv/_bulk call (same ohlcv_adjusted table, identical date-range
    semantics) and filter to the requested universe/min_history_days
    client-side. Row order becomes (ticker, date) alphabetical rather than
    get_tickers()'s list order — confirmed harmless, nothing downstream
    indexes into this DataFrame positionally.
    """
    client = DataStoreClient()
    tickers = get_tickers()
    if max_tickers:
        tickers = tickers[:max_tickers]
    ticker_set = set(tickers)

    # DataStoreClient.get_ohlcv_bulk calls .date() on its from_date/to_date
    # args (it's typed for datetime) — a plain date.fromisoformat() argparse
    # value has no .date() method, so these must be Timestamps, not dates.
    from_dt = pd.Timestamp(start_date)
    to_dt = pd.Timestamp(end_date)

    bulk = client.get_ohlcv_bulk(from_dt, to_dt)
    bulk = bulk[bulk["ticker"].isin(ticker_set)][["date", "ticker", "close"]]

    counts = bulk.groupby("ticker").size()
    keep = counts[counts >= min_history_days].index
    ohlcv = bulk[bulk["ticker"].isin(keep)].reset_index(drop=True)

    if ohlcv.empty:
        raise ValueError(f"no ticker in the universe has >= {min_history_days} rows of real OHLCV in [{start_date}, {end_date}]")

    logger.info(f"real data: {ohlcv['ticker'].nunique()}/{len(tickers)} universe tickers had >= {min_history_days} rows")
    return ohlcv


# Tolerance window (calendar days) a ticker's most recent real OHLCV bar
# may be behind `as_of` and still count as "currently tradeable" —
# absorbs ordinary trading holidays / a short individual suspension
# without excluding a stock that's still genuinely listed. A gap wider
# than this (no bar at all in the last ~2 weeks) means the ticker hasn't
# listed yet or has already delisted/stopped trading as of `as_of`.
UNIVERSE_STALENESS_TOLERANCE_DAYS = 10


def _build_config(ohlcv: pd.DataFrame, sector_map: Dict[str, str]) -> OrchestratorConfig:
    trading_days = pd.DatetimeIndex(sorted(ohlcv["date"].unique()))
    price_map = {(row.ticker, row.date.date()): row.close for row in ohlcv.itertuples(index=False)}

    # Each ticker's real trading dates as sorted int64 day-ordinals — a
    # fast per-(ticker, as_of) presence check via binary search, built
    # once from the OHLCV already fetched (see module docstring).
    ticker_dates: Dict[str, np.ndarray] = {
        ticker: np.sort(group["date"].to_numpy().astype("datetime64[D]").astype(np.int64))
        for ticker, group in ohlcv.groupby("ticker")
    }

    def universe_provider(as_of: date_type) -> List[str]:
        as_of_ordinal = np.datetime64(as_of, "D").astype(np.int64)
        lower_bound = as_of_ordinal - UNIVERSE_STALENESS_TOLERANCE_DAYS
        candidates = []
        for ticker, dates in ticker_dates.items():
            # Index of the last real bar on/before as_of (searchsorted
            # "right" - 1); a ticker not yet listed at as_of has no such
            # bar at all (idx < 0), and one delisted/suspended too long
            # ago has its last bar older than lower_bound — both excluded.
            idx = np.searchsorted(dates, as_of_ordinal, side="right") - 1
            if idx >= 0 and dates[idx] >= lower_bound:
                candidates.append(ticker)
        return candidates

    return OrchestratorConfig(
        trading_days=trading_days,
        universe_provider=universe_provider,
        price_lookup=lambda ticker, as_of: price_map.get((ticker, as_of)),
        sector_lookup=lambda ticker: sector_map.get(ticker, "Unknown"),
    )


def build_technical_feature_lookup():
    """Callable[[ticker, as_of_date], Dict[str, float]] returning that
    ticker's real technical indicator snapshot (sma_200_ratio, rsi_14,
    adx_14, etc. — the full daily feature Parquet row) as of a date —
    used as BacktestOrchestrator's technical_feature_lookup so
    ConditionBasedExitPolicy sees the SAME indicator values ScreenerEngine/
    TechnicalAdapter already read for entry screening (features/technical.
    py::compute_technical_features, materialized into config.settings.
    FEATURES_DAILY_DIR), never recomputed.

    Caches only the SINGLE most-recently-loaded date's feature Parquet, not
    every date ever seen. This closure is built once per run and its
    `lookup` is called by BacktestOrchestrator._apply_exit_policy() — which
    runs every trading day, for every open position — for the entire life
    of the run, walking `as_of` strictly forward in calendar order (see
    core/engine.py's `for as_of_date in config.trading_days:` loop). A plain
    `dict` keyed by date string (the original implementation) therefore
    accumulated one full ~2,300-ticker DataFrame PER TRADING DAY with no
    eviction — confirmed live via py-spy on a 10-year/800-ticker run
    (2026-07-25, FeatureBacklog.md): ~2,500 trading days' worth of
    never-freed DataFrames drove memory from a few hundred MB up past a 6GB
    cgroup cap over ~1.5 hours, independent of exit-policy variant (this
    lookup fires for every technical-channel run, not just
    regime_conditional — that variant's OWN unrelated performance bug,
    fixed the same session in regime_conditional_exit_policy.py, is what
    surfaced this one by making a run run long enough to observe it).
    Bounding the cache to size 1 is correct (not just a size limit that
    happens to work): a date once passed is never looked up again within a
    single run, so evicting anything but the current date loses nothing."""
    from systems.technical_analysis.screener.engine import ScreenerEngine

    engine = ScreenerEngine()
    cached_date: Optional[str] = None
    cached_df: Optional[pd.DataFrame] = None

    def lookup(ticker: str, as_of: date_type) -> Dict[str, float]:
        nonlocal cached_date, cached_df
        date_str = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
        if date_str != cached_date:
            cached_date = date_str
            cached_df = engine._load_df(date_str)
        df = cached_df
        if df is None or "ticker" not in df.columns:
            return {}
        row = df.loc[df["ticker"] == ticker]
        if row.empty:
            return {}
        return row.iloc[0].to_dict()

    return lookup


def _resolve_horizon_bucket(
    channel: str, horizon_bucket: Optional[str], template_name: Optional[str], preset: Optional[str],
    lookback_months: int,
) -> HorizonBucket:
    """Resolves the run's HorizonBucket: an explicit --horizon-bucket wins
    (full HorizonBucket value or a strategy_id short code, e.g. "21d");
    omitted, it defaults per channel — Technical from the template's real
    TEMPLATE_STYLE via the Explainer's published style->horizon table,
    Fundamental from its preset, Momentum from its lookback_months (see
    backtest/strategy_id.py's default_horizon_for_* docstrings)."""
    if horizon_bucket:
        if horizon_bucket in HORIZON_BUCKET_MAP:
            return HORIZON_BUCKET_MAP[horizon_bucket]
        if horizon_bucket in CODE_TO_HORIZON:
            return CODE_TO_HORIZON[horizon_bucket]
        raise ValueError(
            f"unknown horizon_bucket {horizon_bucket!r}; must be one of {list(HORIZON_BUCKET_MAP)} "
            f"or a short code {list(CODE_TO_HORIZON)}"
        )

    if channel == "technical":
        if not template_name or template_name not in TEMPLATE_STYLE:
            raise ValueError(f"cannot default horizon_bucket: unknown --template-name {template_name!r}")
        return default_horizon_for_technical(TEMPLATE_STYLE[template_name])
    if channel == "fundamental":
        if not preset:
            raise ValueError("cannot default horizon_bucket: --preset is required for channel=fundamental")
        return default_horizon_for_fundamental(preset)
    if channel == "momentum":
        return default_horizon_for_momentum(lookback_months)
    raise ValueError(f"unsupported channel {channel!r} — must be technical, fundamental, or momentum")


@contextlib.contextmanager
def _no_regime_conn():
    yield None


def run_orchestrator_backtest(
    channel: str, start_date: date_type, end_date: date_type, strategy_id: Optional[str] = None,
    horizon_bucket: Optional[str] = None,
    capital_mode: str = "lump", initial_capital: float = 1_000_000.0, sip_amount: Optional[float] = None,
    universe_spec: str = "curated", max_tickers: Optional[int] = None, min_history_days: int = 60,
    template_name: Optional[str] = None, preset: Optional[str] = None, top_n: int = 10,
    lookback_months: int = 6, run_id: Optional[str] = None, report_suffix: Optional[str] = None,
    regime_index_name: Optional[str] = "Nifty 500",
    exit_policy_variant: str = "baseline",
    regime_method: Optional[str] = None,
) -> dict:
    horizon = _resolve_horizon_bucket(channel, horizon_bucket, template_name, preset, lookback_months)

    run_started = time.monotonic()
    run_date = now_ist()
    run_id = run_id or f"orch_{channel}_{run_date.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    # descriptor is the channel-specific "what strategy is this" bit: the
    # template name, the preset name, or a top-N/lookback summary for
    # momentum (which has no single named descriptor of its own). Computed
    # unconditionally (not just when strategy_id is auto-built) since
    # strategy_catalog needs it regardless of whether the caller passed an
    # explicit strategy_id.
    descriptor = {
        "technical": template_name, "fundamental": preset,
        "momentum": f"top{top_n}_{lookback_months}m",
    }[channel]
    if not strategy_id:
        # Codified strategy_id (backtest/strategy_id.py)
        strategy_id = build_strategy_id(channel, descriptor, horizon, as_of=run_date.date())

    logger.info(
        f"orchestrator backtest starting: channel={channel} strategy_id={strategy_id} "
        f"run_id={run_id} horizon_bucket={horizon.value}"
    )
    # Held across the entire real-work window (data fetch through DB write)
    # — user-confirmed requirement: backtests run strictly sequentially,
    # never concurrently, even across independently-triggered queues/direct
    # triggers (see batch_common.exclusive_backtest_lock's docstring: two
    # concurrently-running queues previously starved each other on DB-lock
    # contention and started failing outright).
    with exclusive_backtest_lock(label=f"orchestrator[{run_id}]"):
        ohlcv = _fetch_real_ohlcv(max_tickers, min_history_days, start_date, end_date)
        sector_map = _real_sector_map()
        config = _build_config(ohlcv, sector_map)

        if channel == "technical":
            if not template_name:
                raise ValueError("channel=technical requires --template-name")
            adapter = TechnicalAdapter(template_name=template_name, top_n=top_n, sector_lookup=sector_map)
        elif channel == "fundamental":
            if not preset:
                raise ValueError("channel=fundamental requires --preset")
            adapter = FundamentalAdapter(preset=preset, top_n=top_n, sector_lookup=sector_map)
        elif channel == "momentum":
            price_panel = ohlcv.pivot(index="date", columns="ticker", values="close")
            adapter = MomentumAdapter(price_panel=price_panel, top_n=top_n, lookback_months=lookback_months, sector_lookup=sector_map)
        else:
            raise ValueError(f"unsupported channel {channel!r} — must be technical, fundamental, or momentum")

        run = BacktestRun(
            run_id=run_id, channel=channel, strategy_id=strategy_id, horizon_bucket=horizon,
            mode="backtest", universe_spec=universe_spec, start_date=start_date, end_date=end_date,
            capital_mode=capital_mode, initial_capital=initial_capital, sip_amount=sip_amount,
            config={
                "template_name": template_name, "preset": preset, "top_n": top_n, "lookback_months": lookback_months,
                "max_tickers": max_tickers, "min_history_days": min_history_days, "exit_variant": exit_policy_variant,
            },
        )

        create_backtest_schema(
            BACKTEST_DUCKDB_PATH,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        )
        # market_regimes lives in the normalised-schema DB (DUCKDB_PATH), a
        # separate file from BACKTEST_DUCKDB_PATH — a second, read-only
        # connection, opened only when a regime breakdown was actually
        # requested (regime_index_name=None skips it entirely).
        regime_cm = (
            get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
            if regime_index_name
            else _no_regime_conn()
        )
        # [2026-07-25 fix] piotroski_on_value/margin_of_safety/net_net need
        # raw fundamentals_history + ohlcv_adjusted from the normalised-
        # schema DB (DUCKDB_PATH) — FundamentalAdapter previously required
        # this at construction time (before any DB connection exists here),
        # so orchestrator CLI runs of these 3 presets raised ValueError
        # immediately. Same read-only-second-connection shape as regime_cm.
        fundamentals_cm = (
            get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
            if channel == "fundamental" and preset in BESPOKE_PRESETS
            else _no_regime_conn()
        )
        # 2026-07-26 fix: a wider lock-retry budget than the API's default
        # (DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS/_BASE_DELAY_S vs.
        # DUCKDB_LOCK_RETRY_ATTEMPTS/_BASE_DELAY_S) — this job has no outer
        # timeout, unlike the API's read-only requests, so it can afford to
        # wait out sustained read-lock churn from frontend status polling
        # rather than hard-failing after ~15.5s. Reviewed by
        # ml-rigor-reviewer + backtest-reviewer (see FeatureBacklog.md).
        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn, regime_cm as regime_conn, fundamentals_cm as fundamentals_conn:
            if channel == "technical":
                # Wired post-construction (conn doesn't exist yet when
                # `adapter` is built above) — shares entry-signal candidates
                # across every exit-variant job for the same template via
                # technical_screener_cache (backtest/core/screener_cache.py,
                # 2026-07-25 fix — see FeatureBacklog.md).
                adapter._screener_cache_conn = conn
            elif channel == "fundamental" and preset in BESPOKE_PRESETS:
                # Same deferred-wiring pattern as the technical branch above.
                adapter._db_conn = fundamentals_conn
            feature_log_writer = FeatureLogWriter(conn)
            exit_model = build_exit_model_for_variant(
                exit_policy_variant, regime_conn=regime_conn, regime_index_name=regime_index_name or "Nifty 500",
            )
            # ConditionBasedExitPolicy (and CompositeExitPolicy wrapping it)
            # need live technical indicator values per ticker/day — only
            # wired for channel=="technical" (the only channel with a real
            # screener-template `template` to re-check); other channels'
            # exit_ctx rows simply won't carry these columns, which
            # ConditionBasedExitPolicy already treats as "never triggers".
            technical_feature_lookup = build_technical_feature_lookup() if channel == "technical" else None
            # Tag the saved exit_policy_variant with a non-default regime
            # method so experiments comparing 5/10/15/20% thresholds against
            # the SAME strategy+variant stay distinguishable in backtest_runs
            # without a schema migration (regime_method itself isn't a
            # persisted column — see BacktestOrchestrator.__init__ docstring).
            from systems.regime.market_regime import METHOD_NAME as _DEFAULT_REGIME_METHOD
            saved_exit_policy_variant = exit_policy_variant
            if regime_method and regime_method != _DEFAULT_REGIME_METHOD:
                saved_exit_policy_variant = f"{exit_policy_variant}__{regime_method}"

            result = BacktestOrchestrator(
                feature_log_writer=feature_log_writer, regime_conn=regime_conn,
                regime_index_name=regime_index_name or "Nifty 500", exit_model=exit_model,
                technical_feature_lookup=technical_feature_lookup, exit_policy_variant=saved_exit_policy_variant,
                regime_method=regime_method,
            ).run(run, adapter, config)
            feature_log_writer.flush()
            save_run_result(conn, result)

            # strategy_catalog upsert (2026-07-24 addition, additive only —
            # does not affect run/simulation logic): one row per distinct
            # strategy CONFIGURATION, keyed on channel+descriptor+params so
            # re-running the same config updates the existing row instead
            # of duplicating.
            create_strategy_catalog_schema(
                BACKTEST_DUCKDB_PATH,
                retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
                retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
                retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
            )
            catalog_params = {
                "template_name": template_name, "preset": preset, "top_n": top_n,
                "lookback_months": lookback_months, "exit_policy_variant": saved_exit_policy_variant,
                "regime_method": regime_method,
            }
            catalog_params_json = json.dumps(catalog_params, default=str)
            strategy_key = hashlib.sha1(f"{channel}|{descriptor}|{catalog_params_json}".encode()).hexdigest()
            conn.execute(
                """
                INSERT INTO strategy_catalog
                    (strategy_key, channel, descriptor, params_json, latest_run_id, first_run_at, last_run_at, n_runs)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT (strategy_key) DO UPDATE SET
                    latest_run_id = excluded.latest_run_id,
                    last_run_at = excluded.last_run_at,
                    n_runs = strategy_catalog.n_runs + 1
                """,
                [strategy_key, channel, descriptor, catalog_params_json, run_id, run_date, run_date],
            )
            conn.commit()

            # Enriched trade-book CSV (entry/exit reason + indicator
            # values) — pure post-processing over data already written
            # above. Reuses `conn` (still open here) rather than opening a
            # second connection to the same file, which DuckDB rejects
            # while a read-write connection is already held.
            if result.trade_log_path:
                export_trade_book(run_id, Path(result.trade_log_path), conn=conn)

    runtime_seconds = time.monotonic() - run_started
    logger.info(f"orchestrator backtest finished in {runtime_seconds:.1f}s: {json.dumps(result.metrics, default=str)}")

    report = result.to_dict()
    report["runtime_seconds"] = runtime_seconds
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = report_suffix or run_id
    report_path = REPORTS_DIR / f"orchestrator_{suffix}.json"
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"\nReport written to {report_path}")
    print(f"run_id={run_id}")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest/core/engine.py's BacktestOrchestrator against real data")
    parser.add_argument("--channel", required=True, choices=["technical", "fundamental", "momentum"])
    parser.add_argument(
        "--strategy-id", default=None,
        help="Defaults to the codified {channel}_{descriptor}_{horizon}_{YYYYMMDD} form (backtest/strategy_id.py)",
    )
    parser.add_argument(
        "--horizon-bucket", default=None, choices=list(HORIZON_BUCKET_MAP) + list(CODE_TO_HORIZON),
        help="Defaults per channel/template per the Explainer's published style table (backtest/strategy_id.py)",
    )
    parser.add_argument("--start-date", required=True, type=date_type.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date_type.fromisoformat)
    parser.add_argument("--capital-mode", default="lump", choices=["lump", "sip"])
    parser.add_argument("--initial-capital", type=float, default=1_000_000.0)
    parser.add_argument("--sip-amount", type=float, default=None)
    parser.add_argument("--universe-spec", default="curated")
    parser.add_argument("--max-tickers", type=int, default=None)
    parser.add_argument("--min-history-days", type=int, default=60)
    parser.add_argument("--template-name", default=None, help="technical channel: one of the 42 screener templates")
    parser.add_argument(
        "--preset", default=None,
        help=(
            "fundamental channel: any of the 26 features.fundamental_composites.STRATEGY_CATALOG "
            "keys — a SCREENER_PRESETS threshold, a BESPOKE_PRESETS raw-financials strategy "
            "(piotroski_on_value/margin_of_safety/net_net), or a SCORE_FUNCTIONS composite "
            "score (QGLP, Moat, Owner Earnings, etc. — ranked top-N by score, not binary pass/fail)"
        ),
    )
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--lookback-months", type=int, default=6, help="momentum channel only")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--report-suffix", default=None)
    parser.add_argument(
        "--regime-index", default="Nifty 500",
        help="Index (in market_regimes) for the per-Bull/Bear/Sideways performance breakdown. Pass '' to skip it.",
    )
    parser.add_argument(
        "--exit-variant", default="baseline", choices=list(EXIT_POLICY_VARIANTS),
        help=(
            "Exit policy variant (backtest/core/engine.py::build_exit_model_for_variant): "
            "baseline (today's PerTemplateExitPolicy, default — no behavior change if omitted), "
            "condition (ConditionBasedExitPolicy), combined (baseline OR condition), "
            "trailing (TrailingStopExitPolicy), atr_adaptive (ATRAdaptiveExitPolicy), "
            "regime_conditional (RegimeConditionalExitPolicy)."
        ),
    )
    parser.add_argument(
        "--regime-method", default=None,
        help=(
            "Which market_regimes classification threshold to use for the exit-policy `regime` "
            "column AND regime_breakdown (systems/regime/market_regime.py): e.g. '20pct_threshold_v1' "
            "(default, matches METHOD_NAME), '15pct_threshold_v1', '10pct_threshold_v1', "
            "'5pct_threshold_v1'. Only matters for --exit-variant regime_conditional and for "
            "per-regime performance breakdown; other variants ignore it."
        ),
    )
    args = parser.parse_args()

    run_orchestrator_backtest(
        channel=args.channel, strategy_id=args.strategy_id, horizon_bucket=args.horizon_bucket,
        start_date=args.start_date, end_date=args.end_date, capital_mode=args.capital_mode,
        initial_capital=args.initial_capital, sip_amount=args.sip_amount, universe_spec=args.universe_spec,
        max_tickers=args.max_tickers, min_history_days=args.min_history_days, template_name=args.template_name,
        preset=args.preset, top_n=args.top_n, lookback_months=args.lookback_months, run_id=args.run_id,
        report_suffix=args.report_suffix, regime_index_name=args.regime_index or None,
        exit_policy_variant=args.exit_variant, regime_method=args.regime_method,
    )


if __name__ == "__main__":
    main()

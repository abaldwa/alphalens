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
import json
import logging
import time
import uuid
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from backtest.adapters.fundamental_adapter import FundamentalAdapter
from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.adapters.technical_adapter import TechnicalAdapter
from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig
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
from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_PATH
from config.timezone import now_ist
from config.universe import get_tickers
from datastore.api.db import get_duckdb_connection
from datastore.client import DataStoreClient
from datastore.schema.create_backtest import create_backtest_schema
from systems.technical_analysis.screener.templates import TEMPLATE_STYLE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

HORIZON_BUCKET_MAP = {b.value: b for b in HorizonBucket}


def _fetch_real_ohlcv(max_tickers: Optional[int], min_history_days: int, start_date: date_type, end_date: date_type) -> pd.DataFrame:
    """Real OHLCV (config.universe.get_tickers()'s curated universe) over
    exactly [start_date, end_date] — the run's own requested window, not
    run_phase1_backtest.py's fixed 5-year lookback."""
    client = DataStoreClient()
    tickers = get_tickers()
    if max_tickers:
        tickers = tickers[:max_tickers]

    # DataStoreClient.get_ohlcv calls .date() on its from_date/to_date args
    # (it's typed for datetime) — a plain date.fromisoformat() argparse
    # value has no .date() method, so these must be Timestamps, not dates.
    from_dt = pd.Timestamp(start_date)
    to_dt = pd.Timestamp(end_date)

    frames = []
    for ticker in tickers:
        rows = client.get_ohlcv(ticker, from_dt, to_dt)
        if len(rows) >= min_history_days:
            df = pd.DataFrame(rows)[["date", "ticker", "close"]]
            frames.append(df)

    if not frames:
        raise ValueError(f"no ticker in the universe has >= {min_history_days} rows of real OHLCV in [{start_date}, {end_date}]")

    ohlcv = pd.concat(frames, ignore_index=True)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
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
) -> dict:
    horizon = _resolve_horizon_bucket(channel, horizon_bucket, template_name, preset, lookback_months)

    run_started = time.monotonic()
    run_date = now_ist()
    run_id = run_id or f"orch_{channel}_{run_date.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    if not strategy_id:
        # Codified strategy_id (backtest/strategy_id.py) — descriptor is
        # the channel-specific "what strategy is this" bit: the template
        # name, the preset name, or a top-N/lookback summary for momentum
        # (which has no single named descriptor of its own).
        descriptor = {
            "technical": template_name, "fundamental": preset,
            "momentum": f"top{top_n}_{lookback_months}m",
        }[channel]
        strategy_id = build_strategy_id(channel, descriptor, horizon, as_of=run_date.date())

    logger.info(
        f"orchestrator backtest starting: channel={channel} strategy_id={strategy_id} "
        f"run_id={run_id} horizon_bucket={horizon.value}"
    )
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
            "max_tickers": max_tickers, "min_history_days": min_history_days,
        },
    )

    create_backtest_schema(BACKTEST_DUCKDB_PATH)
    # market_regimes lives in the normalised-schema DB (DUCKDB_PATH), a
    # separate file from BACKTEST_DUCKDB_PATH — a second, read-only
    # connection, opened only when a regime breakdown was actually
    # requested (regime_index_name=None skips it entirely).
    regime_cm = (
        get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False)
        if regime_index_name
        else _no_regime_conn()
    )
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, read_only=False, persist=False) as conn, regime_cm as regime_conn:
        feature_log_writer = FeatureLogWriter(conn)
        result = BacktestOrchestrator(
            feature_log_writer=feature_log_writer, regime_conn=regime_conn, regime_index_name=regime_index_name or "Nifty 500"
        ).run(run, adapter, config)
        feature_log_writer.flush()
        save_run_result(conn, result)
        conn.commit()

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
    parser.add_argument("--preset", default=None, help="fundamental channel: a features.fundamental_composites.SCREENER_PRESETS key")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--lookback-months", type=int, default=6, help="momentum channel only")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--report-suffix", default=None)
    parser.add_argument(
        "--regime-index", default="Nifty 500",
        help="Index (in market_regimes) for the per-Bull/Bear/Sideways performance breakdown. Pass '' to skip it.",
    )
    args = parser.parse_args()

    run_orchestrator_backtest(
        channel=args.channel, strategy_id=args.strategy_id, horizon_bucket=args.horizon_bucket,
        start_date=args.start_date, end_date=args.end_date, capital_mode=args.capital_mode,
        initial_capital=args.initial_capital, sip_amount=args.sip_amount, universe_spec=args.universe_spec,
        max_tickers=args.max_tickers, min_history_days=args.min_history_days, template_name=args.template_name,
        preset=args.preset, top_n=args.top_n, lookback_months=args.lookback_months, run_id=args.run_id,
        report_suffix=args.report_suffix, regime_index_name=args.regime_index or None,
    )


if __name__ == "__main__":
    main()

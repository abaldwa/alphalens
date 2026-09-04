"""
Framework Backtest Results Schema — the "different table" for framework
reruns (explicit user instruction, 2026-09-04 session start: "There has
to a common nomenclature for the backtest results — we will rerun the
backtests and post the results in a different table").

Lives in the SAME shared store as the legacy backtest_runs/backtest_trades
tables (datastore/backtest_store/backtest.duckdb) — new tables alongside
them, `framework_` prefixed, never touching or renaming the legacy ones.

VERSIONING (added 2026-09-04, explicit user requirement: "store the
version of the strategy, in case we change the strategy mid-way"):
strategy_id (metrics/nomenclature.py) names a run's CONFIGURATION only —
band, top_n, lookback, etc. It says nothing about which CODE produced the
numbers. source_commit / source_commit_dirty (common/git_provenance.py)
are the CODE identity: two runs sharing an identical strategy_id but a
different source_commit ran under different strategy logic and must
never be silently averaged or compared as if they were the same
experiment. This is the same problem strategy_signals.strategy_version
(the legacy ledger) solves for live signal generation — here it is a git
commit rather than an incrementing integer, because the framework's
"version" IS the code, not a manually-bumped counter someone could forget
to bump.

Two tables, same run_id foreign key:
- framework_backtest_runs: one row per BacktestResult (config + metrics)
- framework_backtest_trades: one row per ROUND-TRIP trade (buy_date,
  buy_price, sale_date, sale_price, qty) — same convention as legacy's
  backtest_trades table (explicit user instruction 2026-09-04: "we will
  still store the round trades, buy and sell signals as in the past with
  the date, quantity and price"), NOT one row per buy/sell event. Built
  from Portfolio.trade_log's buy/sell events by pairing them per ticker
  in chronological order (see results/db_writer.py::_build_round_trips())
  — Portfolio holds at most one open position per ticker at a time (see
  backtesting/portfolio.py's Position dict), so pairing is unambiguous. A
  position still open at the end of the backtest gets a row with
  sale_date/sale_price/pnl left NULL, same as an open position would
  read in the legacy table.
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS framework_backtest_runs (
    run_id VARCHAR PRIMARY KEY,
    strategy_id VARCHAR NOT NULL,       -- config identity, e.g. M02_R01_top10_lb12mo_21d_allrisk
    strategy_code VARCHAR NOT NULL,     -- R01, R03, R07..R17
    band_id INTEGER NOT NULL,
    engine VARCHAR NOT NULL,            -- 'native' | 'legacy_normalized'
    source_commit VARCHAR NOT NULL,     -- git SHA the code ran under ("unknown" if git unavailable)
    source_commit_dirty BOOLEAN NOT NULL DEFAULT FALSE,
    framework_version VARCHAR NOT NULL,
    start_date DATE,
    end_date DATE,
    config_json VARCHAR NOT NULL,
    metrics_json VARCHAR NOT NULL,
    trade_count INTEGER NOT NULL DEFAULT 0,
    integrity_passed BOOLEAN NOT NULL DEFAULT FALSE,
    integrity_detail_json VARCHAR,
    data_gaps_json VARCHAR,
    universe_cache_used BOOLEAN,        -- did resolve_universe() hit the pre-built cache or fall back live
    parity_checked BOOLEAN NOT NULL DEFAULT FALSE,  -- has this exact strategy_id+commit been diffed vs legacy
    run_executed_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_framework_runs_strategy_id
    ON framework_backtest_runs (strategy_id);
CREATE INDEX IF NOT EXISTS idx_framework_runs_commit
    ON framework_backtest_runs (source_commit);

CREATE TABLE IF NOT EXISTS framework_backtest_trades (
    run_id VARCHAR NOT NULL,
    ticker VARCHAR NOT NULL,
    qty DOUBLE NOT NULL,
    buy_date DATE NOT NULL,
    buy_price DOUBLE NOT NULL,
    sale_date DATE,                     -- NULL = still open at end of backtest
    sale_price DOUBLE,                  -- NULL = still open at end of backtest
    pnl_inr DOUBLE,                     -- NULL if still open
    pnl_pct DOUBLE,                     -- NULL if still open
    holding_days INTEGER                -- NULL if still open
);
CREATE INDEX IF NOT EXISTS idx_framework_trades_run_id
    ON framework_backtest_trades (run_id);
"""

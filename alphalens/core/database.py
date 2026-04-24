"""
alphalens/core/database.py

Database connection managers for DuckDB (analytical) and SQLite (transactional).

DuckDB  → OHLCV, indicators, market context, strategies, backtests, patterns
SQLite  → portfolio, watchlist, trades, signals log, config, P&L snapshots

Usage:
    from alphalens.core.database import get_duck, get_sqlite, init_databases
    con = get_duck()     # DuckDB connection (thread-local)
    db  = get_sqlite()   # SQLAlchemy session
"""

import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
from loguru import logger
from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, Integer,
    String, Text, create_engine, text, event
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from config.settings import settings


# ── DuckDB ────────────────────────────────────────────────────────────────

_duck_local = threading.local()


def get_duck() -> duckdb.DuckDBPyConnection:
    """Return a thread-local DuckDB connection."""
    if not hasattr(_duck_local, "conn") or _duck_local.conn is None:
        path = Path(settings.duckdb_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        _duck_local.conn = duckdb.connect(str(path))
        # Performance settings for laptop hardware
        _duck_local.conn.execute("SET memory_limit='4GB';")
        _duck_local.conn.execute("SET threads=4;")
        _duck_local.conn.execute("SET enable_progress_bar=false;")
    return _duck_local.conn


@contextmanager
def duck_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager for DuckDB connection."""
    con = get_duck()
    try:
        yield con
    except Exception:
        logger.exception("DuckDB error")
        raise


def init_duckdb():
    """Create all DuckDB tables and indexes."""
    con = get_duck()

    con.execute("""
    CREATE TABLE IF NOT EXISTS nifty200_stocks (
        symbol          VARCHAR PRIMARY KEY,
        name            VARCHAR NOT NULL,
        sector          VARCHAR,
        industry        VARCHAR,
        market_cap_cat  VARCHAR,   -- large_cap | mid_cap | small_cap
        isin            VARCHAR,
        in_nifty50      BOOLEAN DEFAULT false,
        in_nifty100     BOOLEAN DEFAULT false,
        in_nifty200     BOOLEAN DEFAULT true,
        yfinance_symbol VARCHAR,
        kite_token      BIGINT,
        is_active       BOOLEAN DEFAULT true,
        listed_date     DATE,
        updated_at      TIMESTAMP DEFAULT current_timestamp
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS daily_prices (
        date            DATE NOT NULL,
        symbol          VARCHAR NOT NULL,
        open            DOUBLE,
        high            DOUBLE,
        low             DOUBLE,
        close           DOUBLE NOT NULL,
        adj_close       DOUBLE,
        volume          BIGINT,
        delivery_qty    BIGINT,
        delivery_pct    DOUBLE,
        vwap            DOUBLE,
        source          VARCHAR DEFAULT 'yfinance',
        PRIMARY KEY (date, symbol)
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol_date ON daily_prices(symbol, date DESC);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS technical_indicators (
        date            DATE NOT NULL,
        symbol          VARCHAR NOT NULL,
        -- Trend
        ema_9           DOUBLE, ema_20  DOUBLE, ema_50   DOUBLE,
        ema_100         DOUBLE, ema_200 DOUBLE,
        sma_20          DOUBLE, sma_50  DOUBLE, sma_200  DOUBLE,
        macd            DOUBLE, macd_signal DOUBLE, macd_hist DOUBLE,
        adx_14          DOUBLE, plus_di DOUBLE, minus_di DOUBLE,
        supertrend      DOUBLE, supertrend_dir INTEGER,
        psar            DOUBLE,
        ichimoku_tenkan   DOUBLE, ichimoku_kijun   DOUBLE,
        ichimoku_senkou_a DOUBLE, ichimoku_senkou_b DOUBLE,
        ichimoku_chikou   DOUBLE,
        -- Momentum
        rsi_9   DOUBLE, rsi_14  DOUBLE, rsi_21   DOUBLE,
        stoch_k DOUBLE, stoch_d DOUBLE, stoch_rsi DOUBLE,
        williams_r DOUBLE, cci_20 DOUBLE,
        roc_10  DOUBLE, roc_20  DOUBLE, mfi_14   DOUBLE,
        -- Volatility
        bb_upper DOUBLE, bb_mid  DOUBLE, bb_lower DOUBLE,
        bb_pct_b DOUBLE, bb_width DOUBLE,
        atr_14   DOUBLE, atr_21  DOUBLE,
        kc_upper DOUBLE, kc_lower DOUBLE,
        hist_vol_21 DOUBLE, hist_vol_63 DOUBLE,
        -- Volume
        obv          BIGINT, cmf_20 DOUBLE,
        volume_sma20 BIGINT, volume_ratio DOUBLE,
        -- Price structure
        pct_from_52w_high DOUBLE, pct_from_52w_low DOUBLE,
        pct_from_ema200   DOUBLE, rs_nifty200       DOUBLE,
        PRIMARY KEY (date, symbol)
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_ind_symbol_date ON technical_indicators(symbol, date DESC);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS market_context (
        date                DATE PRIMARY KEY,
        -- Nifty200
        nifty200_open       DOUBLE, nifty200_high   DOUBLE,
        nifty200_low        DOUBLE, nifty200_close  DOUBLE,
        nifty200_1d_ret     DOUBLE, nifty200_5d_ret DOUBLE,
        nifty200_20d_ret    DOUBLE,
        nifty200_above_50dma  BOOLEAN,
        nifty200_above_200dma BOOLEAN,
        nifty200_dma50_pct    DOUBLE,
        nifty200_dma200_pct   DOUBLE,
        -- Fear / Volatility
        india_vix           DOUBLE,
        india_vix_1d_chg    DOUBLE,
        india_vix_pct252    DOUBLE,   -- rolling 252d percentile
        -- Global macro
        dxy                 DOUBLE,   -- US Dollar Index
        brent_crude         DOUBLE,
        nasdaq_close        DOUBLE, nasdaq_5d_ret DOUBLE,
        dow_close           DOUBLE,  dow_5d_ret   DOUBLE,
        us_10yr_yield       DOUBLE,
        usd_inr             DOUBLE,
        -- Institutional flows (crores)
        fii_net_buy_sell    DOUBLE,
        dii_net_buy_sell    DOUBLE,
        fii_10d_sum         DOUBLE,
        -- Breadth
        advance_decline_ratio  DOUBLE,
        pcr_nifty              DOUBLE,
        pct_above_50dma        DOUBLE,
        pct_above_200dma       DOUBLE,
        new_52w_highs          INTEGER,
        new_52w_lows           INTEGER,
        -- Sector closes
        sector_it_close        DOUBLE, sector_bank_close   DOUBLE,
        sector_auto_close      DOUBLE, sector_fmcg_close   DOUBLE,
        sector_pharma_close    DOUBLE, sector_metal_close  DOUBLE,
        sector_realty_close    DOUBLE, sector_energy_close DOUBLE,
        sector_infra_close     DOUBLE, sector_psubank_close DOUBLE,
        sector_fin_close       DOUBLE, sector_media_close  DOUBLE
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS market_cycles (
        date            DATE NOT NULL,
        scope           VARCHAR NOT NULL,   -- market | sector | stock
        scope_id        VARCHAR,            -- NULL for market, sector name, or symbol
        cycle           VARCHAR NOT NULL,   -- bull | bear | neutral
        confidence      DOUBLE,
        model_version   VARCHAR DEFAULT 'v1',
        features_json   VARCHAR,
        PRIMARY KEY (date, scope, scope_id)
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cycles_date ON market_cycles(date DESC);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS strategies (
        strategy_id     VARCHAR PRIMARY KEY,
        name            VARCHAR NOT NULL,
        type            VARCHAR,           -- trend | momentum | mean_reversion | breakout | value | volatility
        description     TEXT,
        timeframes      VARCHAR,           -- JSON array: ["swing","medium"]
        best_cycles     VARCHAR,           -- JSON array: ["bull","neutral"]
        entry_rules     VARCHAR,           -- JSON: full rule definition
        exit_rules      VARCHAR,           -- JSON: full rule definition
        stoploss_rules  VARCHAR,           -- JSON: full rule definition
        parameters      VARCHAR,           -- JSON: all configurable params
        -- Performance metrics
        sharpe_ratio    DOUBLE,
        win_rate        DOUBLE,
        max_drawdown    DOUBLE,
        profit_factor   DOUBLE,
        total_trades    INTEGER,
        bull_sharpe     DOUBLE,
        bear_sharpe     DOUBLE,
        neutral_sharpe  DOUBLE,
        -- Meta
        discovered_by   VARCHAR DEFAULT 'seeded',  -- seeded | genetic
        generation      INTEGER DEFAULT 0,          -- genetic generation number
        is_active       BOOLEAN DEFAULT true,
        created_at      TIMESTAMP DEFAULT current_timestamp,
        last_backtested TIMESTAMP
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS backtest_results (
        run_id          VARCHAR PRIMARY KEY,
        strategy_id     VARCHAR NOT NULL,
        symbol          VARCHAR,           -- NULL = all stocks aggregate
        from_date       DATE,
        to_date         DATE,
        timeframe       VARCHAR,
        -- Core metrics
        sharpe_ratio    DOUBLE,
        sortino_ratio   DOUBLE,
        win_rate        DOUBLE,
        profit_factor   DOUBLE,
        max_drawdown    DOUBLE,
        total_return    DOUBLE,
        annualised_ret  DOUBLE,
        total_trades    INTEGER,
        avg_win_pct     DOUBLE,
        avg_loss_pct    DOUBLE,
        expectancy      DOUBLE,
        -- Cycle breakdown
        cycle_breakdown VARCHAR,           -- JSON: {bull: metrics, bear: metrics, neutral: metrics}
        -- Individual trades
        trades_json     VARCHAR,           -- JSON array of all trades
        created_at      TIMESTAMP DEFAULT current_timestamp
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_bt_strategy ON backtest_results(strategy_id);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS stock_patterns (
        symbol          VARCHAR PRIMARY KEY,
        n_states        INTEGER DEFAULT 3,
        state_labels    VARCHAR,           -- JSON: {0: "accumulation", 1: "trending", 2: "distribution"}
        transition_matrix VARCHAR,         -- JSON matrix
        emission_params VARCHAR,           -- JSON: means and covariances
        current_state   INTEGER,
        state_history   VARCHAR,           -- JSON: [(date, state), ...]
        model_path      VARCHAR,
        fitted_at       TIMESTAMP,
        score           DOUBLE             -- log-likelihood
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS fundamentals (
        symbol              VARCHAR NOT NULL,
        period_end          DATE NOT NULL,
        period_type         VARCHAR DEFAULT 'quarterly',
        pe_ratio            DOUBLE, forward_pe   DOUBLE,
        pb_ratio            DOUBLE, ps_ratio      DOUBLE,
        market_cap_cr       DOUBLE,
        eps                 DOUBLE, eps_growth_yoy DOUBLE,
        roe                 DOUBLE, roce           DOUBLE,
        debt_equity         DOUBLE, current_ratio  DOUBLE,
        revenue_cr          DOUBLE, revenue_growth DOUBLE,
        net_profit_margin   DOUBLE,
        promoter_holding    DOUBLE,
        fii_holding         DOUBLE, dii_holding    DOUBLE,
        scraped_at          TIMESTAMP,
        PRIMARY KEY (symbol, period_end, period_type)
    );
    """)

    logger.info("DuckDB schema initialised")


# ── SQLite ────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class Stock(Base):
    __tablename__ = "stocks"
    symbol   = Column(String, primary_key=True)
    name     = Column(String)
    sector   = Column(String)


class PortfolioHolding(Base):
    __tablename__ = "portfolio_holdings"
    holding_id      = Column(Integer, primary_key=True, autoincrement=True)
    symbol          = Column(String, nullable=False)
    timeframe       = Column(String, nullable=False)  # intraday|swing|medium|long_term
    qty             = Column(Integer, nullable=False)
    avg_cost        = Column(Float, nullable=False)
    entry_date      = Column(Date)
    strategy_id     = Column(String)
    current_target  = Column(Float)
    current_stop_loss = Column(Float)
    trailing_sl     = Column(Float)
    last_reviewed_at = Column(DateTime)
    source          = Column(String, default="manual")  # manual|zerodha_csv|system
    notes           = Column(Text)
    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime)
    updated_at      = Column(DateTime)


class Watchlist(Base):
    __tablename__ = "watchlist"
    watch_id        = Column(Integer, primary_key=True, autoincrement=True)
    symbol          = Column(String, nullable=False)
    timeframe       = Column(String, nullable=False)
    strategy_id     = Column(String)
    signal_type     = Column(String)     # buy | sell
    suggested_entry = Column(Float)
    target_price    = Column(Float)
    stop_loss       = Column(Float)
    risk_reward     = Column(Float)
    confidence      = Column(Float)
    cycle_context   = Column(String)
    sector          = Column(String)
    pattern_state   = Column(String)
    reasoning       = Column(Text)
    valid_till      = Column(Date)
    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime)
    updated_at      = Column(DateTime)


class ClosedTrade(Base):
    __tablename__ = "closed_trades"
    trade_id        = Column(Integer, primary_key=True, autoincrement=True)
    symbol          = Column(String, nullable=False)
    timeframe       = Column(String, nullable=False)
    qty             = Column(Integer)
    entry_date      = Column(Date)
    entry_price     = Column(Float)
    exit_date       = Column(Date)
    exit_price      = Column(Float)
    booked_pnl      = Column(Float)
    booked_pnl_pct  = Column(Float)
    holding_days    = Column(Integer)
    tax_type        = Column(String)    # STCG | LTCG (>1yr = LTCG)
    strategy_id     = Column(String)
    exit_reason     = Column(String)   # target|stop_loss|manual|signal_reversal|monthly_review
    created_at      = Column(DateTime)


class SignalLog(Base):
    __tablename__ = "signals_log"
    signal_id           = Column(Integer, primary_key=True, autoincrement=True)
    generated_at        = Column(DateTime, nullable=False)
    symbol              = Column(String, nullable=False)
    timeframe           = Column(String, nullable=False)
    signal_type         = Column(String)    # buy | sell | hold | exit | review
    strategy_id         = Column(String)
    entry_price         = Column(Float)
    target_price        = Column(Float)
    stop_loss           = Column(Float)
    risk_reward         = Column(Float)
    confidence          = Column(Float)
    cycle_context       = Column(String)
    pattern_state       = Column(String)
    reasoning           = Column(Text)
    notified_telegram   = Column(Boolean, default=False)
    notified_email      = Column(Boolean, default=False)
    is_active           = Column(Boolean, default=True)


class PnlSnapshot(Base):
    __tablename__ = "pnl_snapshots"
    snapshot_id     = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_date   = Column(Date, nullable=False)
    timeframe       = Column(String, nullable=False)  # intraday|swing|medium|long_term|total
    booked_pnl      = Column(Float, default=0.0)
    notional_pnl    = Column(Float, default=0.0)
    total_pnl       = Column(Float, default=0.0)
    portfolio_value = Column(Float)
    invested_capital = Column(Float)
    cash_available  = Column(Float)
    open_positions  = Column(Integer)
    closed_trades   = Column(Integer)


class Config(Base):
    __tablename__ = "config"
    key         = Column(String, primary_key=True)
    value       = Column(Text, nullable=False)
    value_type  = Column(String, default="string")  # string|int|float|bool|json
    description = Column(Text)
    updated_at  = Column(DateTime)


class ZerodhaImport(Base):
    __tablename__ = "zerodha_imports"
    import_id   = Column(Integer, primary_key=True, autoincrement=True)
    import_type = Column(String)    # holdings | tradebook
    filename    = Column(String)
    rows_imported = Column(Integer)
    imported_at = Column(DateTime)
    notes       = Column(Text)


_sqlite_engine = None
_SessionLocal  = None


def get_sqlite_engine():
    global _sqlite_engine
    if _sqlite_engine is None:
        path = Path(settings.sqlite_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        _sqlite_engine = create_engine(
            f"sqlite:///{path}",
            connect_args={"check_same_thread": False},
            echo=False
        )
        # Enable WAL mode for concurrent reads
        @event.listens_for(_sqlite_engine, "connect")
        def set_wal(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA journal_mode=WAL;")
            dbapi_conn.execute("PRAGMA foreign_keys=ON;")
    return _sqlite_engine


def get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_sqlite_engine(), autocommit=False, autoflush=False)
    return _SessionLocal


@contextmanager
def get_sqlite() -> Generator[Session, None, None]:
    """Context manager for SQLite session."""
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("SQLite error")
        raise
    finally:
        session.close()


def init_sqlite():
    """Create all SQLite tables and seed default config."""
    engine = get_sqlite_engine()
    Base.metadata.create_all(engine)

    # Seed default config values
    _seed_config()
    logger.info("SQLite schema initialised")


def _seed_config():
    """Insert default config values if they don't exist."""
    from datetime import datetime
    defaults = [
        ("intraday_slots",   "3",          "int",   "Max intraday portfolio positions"),
        ("swing_slots",      "5",          "int",   "Max swing portfolio positions"),
        ("medium_slots",     "8",          "int",   "Max medium-term portfolio positions"),
        ("longterm_slots",   "15",         "int",   "Max long-term investment positions"),
        ("total_capital",    "2500000",    "float", "Total trading capital in INR"),
        ("intraday_capital", "250000",     "float", "Capital allocated to intraday"),
        ("swing_capital",    "500000",     "float", "Capital allocated to swing"),
        ("medium_capital",   "750000",     "float", "Capital allocated to medium-term"),
        ("longterm_capital", "1000000",    "float", "Capital allocated to long-term investment"),
        ("signal_threshold_bull",    "0.65", "float", "Min confidence for buy signal in bull market"),
        ("signal_threshold_neutral", "0.75", "float", "Min confidence for buy signal in neutral market"),
        ("signal_threshold_bear",    "0.85", "float", "Min confidence for buy signal in bear market"),
        ("min_risk_reward",  "1.5",        "float", "Minimum R:R ratio for signal to fire"),
        ("drawdown_alert_pct", "0.10",     "float", "Drawdown % to trigger investment review alert"),
        ("strategy_min_sharpe", "1.0",     "float", "Min Sharpe for strategy to be activated"),
        ("strategy_min_winrate", "0.52",   "float", "Min win rate for strategy to be activated"),
        ("strategy_min_trades", "50",      "int",   "Min trades in backtest for strategy validation"),
        ("kite_orders_mode", "disabled",   "string","disabled | dry_run | live"),
    ]

    with get_sqlite() as session:
        for key, value, vtype, desc in defaults:
            exists = session.get(Config, key)
            if not exists:
                session.add(Config(
                    key=key, value=value, value_type=vtype,
                    description=desc, updated_at=datetime.now()
                ))


def init_databases():
    """Initialise both databases. Call once at startup."""
    settings.ensure_dirs()
    init_duckdb()
    init_sqlite()
    logger.info("All databases initialised successfully")


def get_config(key: str, default=None):
    """Read a config value from SQLite config table."""
    with get_sqlite() as session:
        row = session.get(Config, key)
        if row is None:
            return default
        if row.value_type == "int":
            return int(row.value)
        elif row.value_type == "float":
            return float(row.value)
        elif row.value_type == "bool":
            return row.value.lower() in ("true", "1", "yes")
        elif row.value_type == "json":
            import json
            return json.loads(row.value)
        return row.value


def set_config(key: str, value, description: str = None):
    """Write a config value to SQLite config table."""
    from datetime import datetime
    import json
    with get_sqlite() as session:
        row = session.get(Config, key)
        if row is None:
            row = Config(key=key)
            session.add(row)
        row.value = str(value) if not isinstance(value, dict) else json.dumps(value)
        row.updated_at = datetime.now()
        if description:
            row.description = description

"""
tests/unit/test_readiness.py

Phase: cross-cutting
Owner: Platform / Architecture
Consumers: CI / `pytest tests/unit/test_readiness.py`

Behavioural tests for backtest/core/readiness.py -- the gate that makes a
signal generator refuse to run on incomplete inputs.

IN-MEMORY DUCKDB ONLY. Project policy is that no test ever writes a row into
the real DuckDB files, not even one it deletes afterwards: a stray fixture row
in ohlcv_adjusted or fundamentals is indistinguishable from real data
downstream and would silently contaminate a backtest. Every connection here
comes from `get_duckdb_connection(None, ...)`, which is `:memory:`, and every
file path (feature panels, model registry) is a pytest tmp_path.

PIT Assumptions
----------------
Fixture fundamentals set announcement_date explicitly, since that -- not
quarter_end_date -- is the PIT key the checker filters on.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from backtest.core.readiness import (
    FYERS_COVERAGE_START,
    MissingInput,
    Readiness,
    ReadinessChecker,
    ReadinessError,
    extract_predicate_features,
    list_blocked,
    record_blocked,
    strategy_indicators,
)
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.registry import register_strategy

AS_OF = date(2026, 8, 12)
UNIVERSE = ["RELIANCE", "TCS", "INFY"]


# ---------------------------------------------------------------------------
# fixtures -- all in-memory / tmp_path
# ---------------------------------------------------------------------------


@pytest.fixture
def market_conn():
    """An in-memory stand-in for the market DB, with just the two tables the
    checker reads. persist is irrelevant for :memory: but read_only is passed
    explicitly because the quality gate wants the intent visible."""
    with get_duckdb_connection(None, read_only=False) as conn:
        conn.execute(
            """
            CREATE TABLE ohlcv_adjusted (
                date DATE, ticker VARCHAR, close DOUBLE, source VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE fundamentals (
                ticker VARCHAR,
                announcement_date DATE,
                ebit DOUBLE, net_debt DOUBLE, debt_to_ebitda DOUBLE,
                fcf_margin DOUBLE, capex_intensity DOUBLE
            )
            """
        )
        yield conn
        conn.execute("DROP TABLE ohlcv_adjusted")
        conn.execute("DROP TABLE fundamentals")


@pytest.fixture
def registry_conn():
    """In-memory strategy_registry. create_strategy_registry_schema(in_memory=
    True) opens its own :memory: connection, which get_duckdb_connection
    caches -- so the same call returns the same database here."""
    create_strategy_registry_schema(in_memory=True)
    with get_duckdb_connection(None, read_only=False) as conn:
        yield conn
        conn.execute("DELETE FROM strategy_registry")
        conn.execute("DELETE FROM signal_generation_blocked")


def _seed_ohlcv(conn: Any, tickers: List[str], source: str = "fyers") -> None:
    for t in tickers:
        conn.execute(
            "INSERT INTO ohlcv_adjusted VALUES (?, ?, ?, ?)", [AS_OF, t, 100.0, source]
        )


def _seed_fundamentals(
    conn: Any, tickers: List[str], *, announced: date, with_ratios: bool = True
) -> None:
    ratios = [1.0, 2.0, 3.0, 0.1, 0.2] if with_ratios else [1.0, 2.0, None, 0.1, 0.2]
    for t in tickers:
        conn.execute(
            "INSERT INTO fundamentals VALUES (?, ?, ?, ?, ?, ?, ?)",
            [t, announced, *ratios],
        )


def _write_panel(
    tmp_path: Path, columns: Dict[str, Any], as_of: date = AS_OF
) -> Path:
    panel_dir = tmp_path / "daily"
    panel_dir.mkdir(exist_ok=True)
    path = panel_dir / f"{as_of.isoformat()}.parquet"
    pd.DataFrame({"ticker": UNIVERSE, **columns}).to_parquet(path)
    return panel_dir


def _register_technical(conn: Any, name: str, features: List[str]) -> str:
    return register_strategy(
        channel="technical",
        name=name,
        display_label=name,
        definition={"template": name, "horizon_days": 21},
        entry_criterion=[{"feature": f, "op": "gt", "value": 0} for f in features],
        exit_criterion={"variant": "fixed", "max_hold_days": 21},
        status="active",
        # In force before AS_OF: the checker reads the definition AS OF the
        # signal date, so a strategy registered after that date is correctly
        # invisible to it.
        valid_from=AS_OF - timedelta(days=30),
        conn=conn,
    )


def _checker(**kwargs: Any) -> ReadinessChecker:
    kwargs.setdefault("required_ohlcv_source", "fyers")
    return ReadinessChecker(**kwargs)


# ---------------------------------------------------------------------------
# registry-derived indicator extraction -- the design point that keeps the
# gate from drifting away from the strategy definition
# ---------------------------------------------------------------------------


def test_extract_predicate_features_preserves_order_and_dedupes():
    features = extract_predicate_features(
        [
            {"feature": "rsi_14", "op": "lt", "value": 30},
            {"feature": "adx_14", "op": "gt", "value": 25},
            {"feature": "rsi_14", "op": "gt", "value": 10},
        ]
    )
    assert features == ["rsi_14", "adx_14"]


def test_extract_predicate_features_walks_nested_groups():
    # A feature referenced only inside an OR branch is still read by the
    # engine; skipping it would reintroduce the silent-NULL failure.
    features = extract_predicate_features(
        [{"any": [{"feature": "macd_hist", "op": "gt", "value": 0}]}]
    )
    assert features == ["macd_hist"]


def test_strategy_indicators_come_from_the_registry(registry_conn):
    key = _register_technical(registry_conn, "gate_derived", ["rsi_14", "atr_14"])
    assert strategy_indicators(key, registry_conn=registry_conn) == ["rsi_14", "atr_14"]


def test_strategy_indicators_track_a_revision(registry_conn):
    """The whole point: revising the strategy changes what the gate demands,
    with no edit to readiness.py."""
    from strategies.registry import revise_strategy

    key = _register_technical(registry_conn, "gate_revised", ["rsi_14"])
    revise_strategy(
        key,
        entry_criterion=[{"feature": "cci_20", "op": "gt", "value": 100}],
        conn=registry_conn,
    )
    assert strategy_indicators(key, registry_conn=registry_conn) == ["cci_20"]


def test_strategy_indicators_unknown_strategy_raises(registry_conn):
    with pytest.raises(ReadinessError):
        strategy_indicators("technical:does_not_exist", registry_conn=registry_conn)


# ---------------------------------------------------------------------------
# momentum contract: ohlcv only
# ---------------------------------------------------------------------------


def test_momentum_ready_when_ohlcv_complete(market_conn):
    _seed_ohlcv(market_conn, UNIVERSE)
    result = _checker(market_conn=market_conn).check(
        "momentum", AS_OF, universe=UNIVERSE
    )
    assert result.ready
    assert result.missing == []


def test_momentum_blocked_when_a_ticker_is_absent(market_conn):
    _seed_ohlcv(market_conn, UNIVERSE[:2])
    result = _checker(market_conn=market_conn).check(
        "momentum", AS_OF, universe=UNIVERSE
    )
    assert not result.ready
    assert [m.kind for m in result.missing] == ["ohlcv"]
    assert "INFY" in result.missing[0].detail


def test_momentum_blocked_on_wrong_source_despite_full_row_count(market_conn):
    """The FYERS token expiry shape: every row present, counts normal, and
    the data silently came from the bhavcopy fallback."""
    _seed_ohlcv(market_conn, UNIVERSE, source="bhavcopy")
    result = _checker(market_conn=market_conn).check(
        "momentum", AS_OF, universe=UNIVERSE
    )
    assert not result.ready
    assert result.missing[0].expected == "source = 'fyers'"


def test_pre_2017_legacy_rows_are_not_blocked_on_provenance(market_conn):
    """Fyers coverage starts 2017-01-01; every earlier row is legacy with
    source NULL, permanently. Applying the provenance check there would block
    every backtest date from the 2009 start to 2017 -- most of the track
    record -- for a condition no ingestion run can ever satisfy."""
    legacy_date = date(2013, 6, 3)
    for t in UNIVERSE:
        market_conn.execute(
            "INSERT INTO ohlcv_adjusted VALUES (?, ?, ?, ?)", [legacy_date, t, 100.0, None]
        )
    result = _checker(market_conn=market_conn).check(
        "momentum", legacy_date, universe=UNIVERSE
    )
    assert result.ready, result.missing


def test_provenance_still_enforced_from_the_fyers_start_date(market_conn):
    """The date carve-out must not become a blanket exemption -- on the first
    covered date a bhavcopy fallback is still a block."""
    for t in UNIVERSE:
        market_conn.execute(
            "INSERT INTO ohlcv_adjusted VALUES (?, ?, ?, ?)",
            [FYERS_COVERAGE_START, t, 100.0, "bhavcopy"],
        )
    result = _checker(market_conn=market_conn).check(
        "momentum", FYERS_COVERAGE_START, universe=UNIVERSE
    )
    assert not result.ready
    assert result.missing[0].expected == "source = 'fyers'"


def test_empty_universe_is_an_error_not_a_pass(market_conn):
    # An empty universe would otherwise vacuously satisfy every coverage
    # check and report ready=True.
    with pytest.raises(ReadinessError):
        _checker(market_conn=market_conn).check("momentum", AS_OF, universe=[])


# ---------------------------------------------------------------------------
# technical contract: ohlcv + panel + registry-derived indicators
# ---------------------------------------------------------------------------


def test_technical_ready_with_all_registered_indicators_present(
    market_conn, registry_conn, tmp_path
):
    key = _register_technical(registry_conn, "gate_ok", ["rsi_14", "adx_14"])
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"rsi_14": [10.0] * 3, "adx_14": [20.0] * 3})

    result = _checker(
        market_conn=market_conn,
        registry_conn=registry_conn,
        features_daily_dir=panel_dir,
    ).check("technical", AS_OF, universe=UNIVERSE, strategy_key=key)
    assert result.ready, result.reason()


def test_technical_blocked_when_one_registered_indicator_is_missing(
    market_conn, registry_conn, tmp_path
):
    """60-of-66: the panel loads, the strategy would evaluate its predicate
    against a column that isn't there."""
    key = _register_technical(registry_conn, "gate_short", ["rsi_14", "adx_14"])
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"rsi_14": [10.0] * 3})

    result = _checker(
        market_conn=market_conn,
        registry_conn=registry_conn,
        features_daily_dir=panel_dir,
    ).check("technical", AS_OF, universe=UNIVERSE, strategy_key=key)
    assert not result.ready
    assert [m.kind for m in result.missing] == ["indicator"]
    assert "adx_14" in result.missing[0].detail


def test_technical_blocked_when_indicator_column_is_all_null(
    market_conn, registry_conn, tmp_path
):
    key = _register_technical(registry_conn, "gate_null", ["rsi_14", "adx_14"])
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(
        tmp_path, {"rsi_14": [10.0] * 3, "adx_14": [None] * 3}
    )

    result = _checker(
        market_conn=market_conn,
        registry_conn=registry_conn,
        features_daily_dir=panel_dir,
    ).check("technical", AS_OF, universe=UNIVERSE, strategy_key=key)
    assert not result.ready
    assert "adx_14" in result.missing[0].detail


def test_technical_blocked_when_panel_is_for_a_different_date(
    market_conn, registry_conn, tmp_path
):
    """A scheduler gap leaves yesterday's panel sitting in the directory;
    'latest available' would generate today's signals from it."""
    key = _register_technical(registry_conn, "gate_stale", ["rsi_14"])
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(
        tmp_path, {"rsi_14": [10.0] * 3}, as_of=AS_OF - timedelta(days=1)
    )

    result = _checker(
        market_conn=market_conn,
        registry_conn=registry_conn,
        features_daily_dir=panel_dir,
    ).check("technical", AS_OF, universe=UNIVERSE, strategy_key=key)
    assert not result.ready
    assert [m.kind for m in result.missing] == ["feature_panel"]


def test_technical_includes_the_momentum_prerequisite(
    market_conn, registry_conn, tmp_path
):
    key = _register_technical(registry_conn, "gate_cumulative", ["rsi_14"])
    _seed_ohlcv(market_conn, UNIVERSE[:1])
    panel_dir = _write_panel(tmp_path, {"rsi_14": [10.0] * 3})

    result = _checker(
        market_conn=market_conn,
        registry_conn=registry_conn,
        features_daily_dir=panel_dir,
    ).check("technical", AS_OF, universe=UNIVERSE, strategy_key=key)
    assert not result.ready
    assert "ohlcv" in {m.kind for m in result.missing}


def test_technical_without_strategy_key_is_an_error(market_conn, tmp_path):
    # Refusing here is what forbids a hardcoded indicator list from creeping
    # back in as a fallback.
    with pytest.raises(ReadinessError):
        _checker(
            market_conn=market_conn, features_daily_dir=tmp_path
        ).check("technical", AS_OF, universe=UNIVERSE)


# ---------------------------------------------------------------------------
# ml contract: technical + full model feature set + current artifact
# ---------------------------------------------------------------------------


def _write_model_registry(
    tmp_path: Path,
    *,
    name: str = "signal_5d",
    last_trained: date = AS_OF,
    interval: int = 28,
    artifact: bool = True,
) -> Path:
    artifact_path = tmp_path / f"{name}.pkl"
    if artifact:
        artifact_path.write_text("artifact")
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                name: {
                    "saved_path": str(artifact_path),
                    "saved_at": datetime.now().isoformat(),
                    "last_trained_date": last_trained.isoformat(),
                    "training_interval_days": interval,
                }
            }
        )
    )
    return path


def test_ml_ready_with_full_feature_set_and_current_artifact(market_conn, tmp_path):
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"ret_5d": [0.1] * 3, "vol_21d": [0.2] * 3})
    registry_path = _write_model_registry(tmp_path)

    result = _checker(
        market_conn=market_conn,
        features_daily_dir=panel_dir,
        model_registry_path=registry_path,
    ).check(
        "ml",
        AS_OF,
        universe=UNIVERSE,
        model_name="signal_5d",
        model_features=["ret_5d", "vol_21d"],
    )
    assert result.ready, result.reason()


def test_ml_blocked_when_a_model_feature_is_absent_from_the_panel(
    market_conn, tmp_path
):
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"ret_5d": [0.1] * 3})
    registry_path = _write_model_registry(tmp_path)

    result = _checker(
        market_conn=market_conn,
        features_daily_dir=panel_dir,
        model_registry_path=registry_path,
    ).check(
        "ml",
        AS_OF,
        universe=UNIVERSE,
        model_name="signal_5d",
        model_features=["ret_5d", "vol_21d"],
    )
    assert not result.ready
    assert "vol_21d" in result.missing[0].detail


def test_ml_blocked_when_artifact_file_is_gone(market_conn, tmp_path):
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"ret_5d": [0.1] * 3})
    registry_path = _write_model_registry(tmp_path, artifact=False)

    result = _checker(
        market_conn=market_conn,
        features_daily_dir=panel_dir,
        model_registry_path=registry_path,
    ).check(
        "ml", AS_OF, universe=UNIVERSE, model_name="signal_5d", model_features=["ret_5d"]
    )
    assert not result.ready
    assert [m.kind for m in result.missing] == ["model_artifact"]


def test_ml_blocked_when_model_is_past_its_retrain_interval(market_conn, tmp_path):
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"ret_5d": [0.1] * 3})
    registry_path = _write_model_registry(
        tmp_path, last_trained=AS_OF - timedelta(days=90), interval=28
    )

    result = _checker(
        market_conn=market_conn,
        features_daily_dir=panel_dir,
        model_registry_path=registry_path,
    ).check(
        "ml", AS_OF, universe=UNIVERSE, model_name="signal_5d", model_features=["ret_5d"]
    )
    assert not result.ready
    assert "last trained" in result.missing[0].detail


def test_ml_blocked_when_model_absent_from_registry(market_conn, tmp_path):
    _seed_ohlcv(market_conn, UNIVERSE)
    panel_dir = _write_panel(tmp_path, {"ret_5d": [0.1] * 3})
    registry_path = _write_model_registry(tmp_path, name="other_model")

    result = _checker(
        market_conn=market_conn,
        features_daily_dir=panel_dir,
        model_registry_path=registry_path,
    ).check(
        "ml", AS_OF, universe=UNIVERSE, model_name="signal_5d", model_features=["ret_5d"]
    )
    assert not result.ready
    assert result.missing[0].kind == "model_artifact"


# ---------------------------------------------------------------------------
# fundamental contract: PIT fundamentals + derived ratios
# ---------------------------------------------------------------------------


def test_fundamental_ready_with_current_pit_rows_and_ratios(market_conn):
    _seed_fundamentals(market_conn, UNIVERSE, announced=AS_OF - timedelta(days=30))
    result = _checker(market_conn=market_conn).check(
        "fundamental", AS_OF, universe=UNIVERSE
    )
    assert result.ready, result.reason()


def test_fundamental_ignores_rows_announced_after_as_of(market_conn):
    """PIT: a result published tomorrow cannot make today ready."""
    _seed_fundamentals(market_conn, UNIVERSE, announced=AS_OF + timedelta(days=1))
    result = _checker(market_conn=market_conn).check(
        "fundamental", AS_OF, universe=UNIVERSE
    )
    assert not result.ready
    assert result.missing[0].kind == "fundamentals"


def test_fundamental_blocked_when_newest_row_is_stale(market_conn):
    _seed_fundamentals(market_conn, UNIVERSE, announced=AS_OF - timedelta(days=400))
    result = _checker(market_conn=market_conn).check(
        "fundamental", AS_OF, universe=UNIVERSE
    )
    assert not result.ready
    assert "older than" in result.missing[0].detail


def test_fundamental_blocked_when_derived_ratios_are_null(market_conn):
    _seed_fundamentals(
        market_conn, UNIVERSE, announced=AS_OF - timedelta(days=30), with_ratios=False
    )
    result = _checker(market_conn=market_conn).check(
        "fundamental", AS_OF, universe=UNIVERSE
    )
    assert not result.ready
    assert "derived ratios" in result.missing[0].detail


# ---------------------------------------------------------------------------
# partial readiness is not a thing
# ---------------------------------------------------------------------------


def test_any_missing_input_blocks():
    ready = Readiness.build(channel="technical", as_of_date=AS_OF, missing=[])
    assert ready.ready
    blocked = Readiness.build(
        channel="technical",
        as_of_date=AS_OF,
        missing=[MissingInput(kind="indicator", detail="rsi_14 null")],
    )
    assert not blocked.ready


def test_unknown_missing_kind_is_rejected():
    with pytest.raises(ReadinessError):
        MissingInput(kind="vibes", detail="felt off")


def test_unknown_channel_is_an_error(market_conn):
    with pytest.raises(ReadinessError):
        _checker(market_conn=market_conn).check("astrology", AS_OF, universe=UNIVERSE)


# ---------------------------------------------------------------------------
# recording refusals
# ---------------------------------------------------------------------------


def test_record_blocked_stores_which_input_was_missing(registry_conn):
    blocked = Readiness.build(
        channel="technical",
        as_of_date=AS_OF,
        missing=[
            MissingInput(kind="indicator", detail="adx_14 is null", expected="panel"),
            MissingInput(kind="ohlcv", detail="INFY absent", expected="3 tickers"),
        ],
    )
    written = record_blocked(
        blocked, strategy_key="technical:gate_rec", strategy_version=1, conn=registry_conn
    )
    assert written == 1

    rows = list_blocked(as_of_date=AS_OF, conn=registry_conn)
    assert len(rows) == 1
    kinds = [m["kind"] for m in rows[0]["missing"]]
    assert kinds == ["indicator", "ohlcv"]
    assert rows[0]["missing"][0]["detail"] == "adx_14 is null"
    assert rows[0]["channel"] == "technical"


def test_record_blocked_is_idempotent_across_retries(registry_conn):
    """The scheduler retries the same evening while a backfill catches up;
    the latest attempt's missing list is the one worth keeping."""
    first = Readiness.build(
        channel="momentum",
        as_of_date=AS_OF,
        missing=[MissingInput(kind="ohlcv", detail="3 absent")],
    )
    second = Readiness.build(
        channel="momentum",
        as_of_date=AS_OF,
        missing=[MissingInput(kind="ohlcv", detail="1 absent")],
    )
    record_blocked(
        first, strategy_key="momentum:gate_retry", strategy_version=1, conn=registry_conn
    )
    record_blocked(
        second, strategy_key="momentum:gate_retry", strategy_version=1, conn=registry_conn
    )

    rows = list_blocked(as_of_date=AS_OF, conn=registry_conn)
    assert len(rows) == 1
    assert rows[0]["missing"][0]["detail"] == "1 absent"


def test_record_blocked_writes_nothing_when_ready(registry_conn):
    ready = Readiness.build(channel="momentum", as_of_date=AS_OF, missing=[])
    assert (
        record_blocked(
            ready, strategy_key="momentum:gate_ok", strategy_version=1, conn=registry_conn
        )
        == 0
    )
    assert list_blocked(as_of_date=AS_OF, conn=registry_conn) == []

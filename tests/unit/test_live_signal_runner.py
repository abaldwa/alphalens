"""tests/unit/test_live_signal_runner.py — backtest/core/live_signal_runner.py (D2)."""

from datetime import date

import duckdb
import pytest

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from backtest.core.live_signal_runner import LiveSignalRunner

AS_OF = date(2026, 7, 20)


@pytest.fixture(autouse=True)
def isolated_ledger(tmp_path, monkeypatch):
    """Never the real DuckDB, not even briefly."""
    import config.settings as settings

    path = tmp_path / "signals_ledger.duckdb"
    monkeypatch.setattr(settings, "BACKTEST_DUCKDB_PATH", path)
    return path


class _FixedAdapter:
    def __init__(self, channel, signals):
        self.channel = channel
        self._signals = signals
        self.calls = []

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        self.calls.append((tuple(universe), as_of_date, horizon_bucket))
        return self._signals

    def feature_vector(self, ticker, as_of_date):
        return {}


class _Readiness:
    def __init__(self, ready, detail="feature store is 3 days stale"):
        self.ready = ready

        class _M:
            pass

        m = _M()
        m.detail = detail
        self.missing = [m]


class _Checker:
    def __init__(self, readiness=None, raises=False):
        self._readiness = readiness or _Readiness(True)
        self._raises = raises

    def check(self, channel, as_of_date, universe=None, strategy_key=None):
        if self._raises:
            raise RuntimeError("checker exploded")
        return self._readiness


def _runner(**kw):
    kw.setdefault("horizon_bucket", HorizonBucket.D5)
    kw.setdefault("enforce_readiness", False)
    kw.setdefault("persist_signals", False)
    return LiveSignalRunner("technical", "ta_A1", **kw)


def _buy(ticker, conviction=0.9):
    return Signal(ticker=ticker, action="buy", sector="Energy", conviction=conviction)


class TestSignalsFor:
    def test_delegates_to_the_adapter_unchanged(self):
        adapter = _FixedAdapter("technical", [_buy("RELIANCE")])
        out = _runner().signals_for(adapter, ["RELIANCE", "TCS"], AS_OF)
        assert [s.ticker for s in out] == ["RELIANCE"]
        assert adapter.calls == [(("RELIANCE", "TCS"), AS_OF, HorizonBucket.D5)]

    def test_channel_mismatch_rejected(self):
        with pytest.raises(ValueError, match="does not match"):
            _runner().signals_for(_FixedAdapter("momentum", []), [], AS_OF)

    def test_missing_horizon_bucket_rejected(self):
        runner = _runner(horizon_bucket=None)
        with pytest.raises(ValueError, match="horizon_bucket"):
            runner.signals_for(_FixedAdapter("technical", []), [], AS_OF)

    def test_target_holdings_keeps_adapter_order_and_drops_non_buys(self):
        signals = [
            _buy("B"), _buy("A"),
            Signal(ticker="C", action="sell", sector="Energy", conviction=0.1),
        ]
        assert _runner().target_holdings(_FixedAdapter("technical", signals), ["A", "B", "C"], AS_OF) == ["B", "A"]


class TestReadinessGate:
    def test_not_ready_returns_empty_without_calling_the_adapter(self):
        adapter = _FixedAdapter("technical", [_buy("RELIANCE")])
        runner = _runner(enforce_readiness=True, readiness_checker=_Checker(_Readiness(False)))
        assert runner.signals_for(adapter, ["RELIANCE"], AS_OF) == []
        assert adapter.calls == []

    def test_a_checker_that_crashes_is_treated_as_not_ready(self):
        adapter = _FixedAdapter("technical", [_buy("RELIANCE")])
        runner = _runner(enforce_readiness=True, readiness_checker=_Checker(raises=True))
        assert runner.signals_for(adapter, ["RELIANCE"], AS_OF) == []
        assert adapter.calls == []

    def test_ready_generates(self):
        adapter = _FixedAdapter("technical", [_buy("RELIANCE")])
        runner = _runner(enforce_readiness=True, readiness_checker=_Checker(_Readiness(True)))
        assert len(runner.signals_for(adapter, ["RELIANCE"], AS_OF)) == 1


class TestLedger:
    def test_signals_are_recorded_with_source_live(self, isolated_ledger):
        runner = _runner(persist_signals=True, signal_ledger_db_path=isolated_ledger)
        runner.signals_for(_FixedAdapter("technical", [_buy("RELIANCE"), _buy("TCS")]), ["RELIANCE", "TCS"], AS_OF)
        with duckdb.connect(str(isolated_ledger)) as conn:
            rows = conn.execute(
                "SELECT ticker, source, strategy_key FROM strategy_signals ORDER BY ticker"
            ).fetchall()
        assert [r[0] for r in rows] == ["RELIANCE", "TCS"]
        assert {r[1] for r in rows} == {"live"}
        assert {r[2] for r in rows} == {"technical:ta_A1"}

    def test_persist_signals_off_writes_nothing(self, isolated_ledger):
        runner = _runner(persist_signals=False, signal_ledger_db_path=isolated_ledger)
        runner.signals_for(_FixedAdapter("technical", [_buy("RELIANCE")]), ["RELIANCE"], AS_OF)
        assert not isolated_ledger.exists()

    def test_a_refused_day_records_no_live_signals(self, isolated_ledger):
        runner = _runner(
            persist_signals=True, signal_ledger_db_path=isolated_ledger,
            enforce_readiness=True, readiness_checker=_Checker(_Readiness(False)),
        )
        runner.signals_for(_FixedAdapter("technical", [_buy("RELIANCE")]), ["RELIANCE"], AS_OF)
        with duckdb.connect(str(isolated_ledger)) as conn:
            tables = {t[0] for t in conn.execute("SHOW TABLES").fetchall()}
            live = 0
            if "strategy_signals" in tables:
                live = conn.execute("SELECT count(*) FROM strategy_signals WHERE source='live'").fetchone()[0]
        assert live == 0

"""tests/unit/test_precompute_technical_screener_matches.py —
scripts/precompute_technical_screener_matches.py. Uses a fake
ScreenerEngine (same pattern as test_technical_adapter.py) so output
shape/idempotency is tested without depending on real feature Parquets."""

import json
from datetime import date

import pandas as pd

from scripts.precompute_technical_screener_matches import (
    _already_covers,
    precompute_template,
    run_precompute,
)


class _FakeResult:
    def __init__(self, ticker, score, matched=3, total=4, key_values=None):
        self.ticker = ticker
        self.score = score
        self.matched_conditions = matched
        self.total_conditions = total
        self.key_values = key_values or {}


class _FakeScreenerEngine:
    def __init__(self, results_by_date):
        self._results_by_date = results_by_date
        self.screen_calls = []

    def screen(self, template_name, date=None, limit=50):
        self.screen_calls.append((template_name, date))
        return self._results_by_date.get(date, [])


class TestPrecomputeTemplate:
    def test_writes_parquet_and_manifest(self, tmp_path):
        engine = _FakeScreenerEngine({
            "2020-01-01": [_FakeResult("A", 0.9, key_values={"rsi_14": 28.0})],
            "2020-01-02": [],
        })
        n_rows = precompute_template(
            "A1", ["2020-01-01", "2020-01-02"], tmp_path, date(2020, 1, 1), date(2020, 1, 2), engine=engine,
        )
        assert n_rows == 1
        assert (tmp_path / "A1.parquet").exists()
        assert (tmp_path / "A1.manifest.json").exists()

        df = pd.read_parquet(tmp_path / "A1.parquet")
        assert list(df["ticker"]) == ["A"]
        assert df["date"].iloc[0] == "2020-01-01"
        assert json.loads(df["key_values_json"].iloc[0]) == {"rsi_14": 28.0}

        manifest = json.loads((tmp_path / "A1.manifest.json").read_text())
        assert manifest["start_date"] == "2020-01-01"
        assert manifest["end_date"] == "2020-01-02"
        assert manifest["trading_days"] == ["2020-01-01", "2020-01-02"]

    def test_zero_match_days_still_produce_a_manifest_covering_them(self, tmp_path):
        # A genuinely-empty template/date-range must still be distinguishable
        # from "never precomputed" — the manifest's trading_days list is
        # what TechnicalAdapter checks, independent of row count.
        engine = _FakeScreenerEngine({})
        n_rows = precompute_template("A1", ["2020-01-01"], tmp_path, date(2020, 1, 1), date(2020, 1, 1), engine=engine)
        assert n_rows == 0
        manifest = json.loads((tmp_path / "A1.manifest.json").read_text())
        assert manifest["trading_days"] == ["2020-01-01"]

    def test_screen_called_once_per_trading_day(self, tmp_path):
        engine = _FakeScreenerEngine({"2020-01-01": [], "2020-01-02": []})
        precompute_template("A1", ["2020-01-01", "2020-01-02"], tmp_path, date(2020, 1, 1), date(2020, 1, 2), engine=engine)
        assert engine.screen_calls == [("A1", "2020-01-01"), ("A1", "2020-01-02")]


class TestAlreadyCovers:
    def test_missing_files_is_not_covered(self, tmp_path):
        assert _already_covers(tmp_path, "A1", date(2020, 1, 1), date(2020, 1, 2)) is False

    def test_exact_range_match_is_covered(self, tmp_path):
        (tmp_path / "A1.parquet").write_bytes(b"")
        (tmp_path / "A1.manifest.json").write_text(json.dumps({"start_date": "2020-01-01", "end_date": "2020-01-02"}))
        assert _already_covers(tmp_path, "A1", date(2020, 1, 1), date(2020, 1, 2)) is True

    def test_narrower_existing_range_is_not_covered(self, tmp_path):
        (tmp_path / "A1.parquet").write_bytes(b"")
        (tmp_path / "A1.manifest.json").write_text(json.dumps({"start_date": "2020-01-01", "end_date": "2020-01-15"}))
        # requested range extends past what's cached
        assert _already_covers(tmp_path, "A1", date(2020, 1, 1), date(2020, 2, 1)) is False

    def test_wider_existing_range_is_covered(self, tmp_path):
        (tmp_path / "A1.parquet").write_bytes(b"")
        (tmp_path / "A1.manifest.json").write_text(json.dumps({"start_date": "2016-01-01", "end_date": "2026-01-01"}))
        assert _already_covers(tmp_path, "A1", date(2020, 1, 1), date(2020, 2, 1)) is True

    def test_corrupt_manifest_is_not_covered(self, tmp_path):
        (tmp_path / "A1.parquet").write_bytes(b"")
        (tmp_path / "A1.manifest.json").write_text("not json")
        assert _already_covers(tmp_path, "A1", date(2020, 1, 1), date(2020, 1, 2)) is False


class TestRunPrecompute:
    def test_skips_already_covered_templates(self, tmp_path, monkeypatch):
        # Pre-seed A1 as already covered; only D4 should get a real screen() pass.
        (tmp_path / "A1.parquet").write_bytes(b"")
        (tmp_path / "A1.manifest.json").write_text(
            json.dumps({"start_date": "2020-01-01", "end_date": "2020-01-02", "trading_days": []})
        )
        monkeypatch.setattr(
            "scripts.precompute_technical_screener_matches._trading_days",
            lambda start, end: ["2020-01-01", "2020-01-02"],
        )
        calls = []

        def _fake_precompute_template(template_name, trading_days, output_dir, start_date, end_date, engine=None):
            calls.append(template_name)
            (output_dir / f"{template_name}.parquet").write_bytes(b"")
            (output_dir / f"{template_name}.manifest.json").write_text(
                json.dumps({"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "trading_days": trading_days})
            )
            return 0

        monkeypatch.setattr(
            "scripts.precompute_technical_screener_matches.precompute_template", _fake_precompute_template,
        )
        run_precompute(date(2020, 1, 1), date(2020, 1, 2), templates=["A1", "D4"], output_dir=tmp_path)
        assert calls == ["D4"]

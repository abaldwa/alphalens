"""
Tests for backtest/artifacts.py — execution-artifact cleanup.

The whole risk of a cleanup tool is deleting something that is still needed, and
here the dangerous case is specific: run reports embed an absolute
`trade_log_path`, and the comparison dataset reads trades back through it. A
cleanup that collects a referenced log turns a published result into a dangling
pointer that only fails much later, at read time. Every protection path is
pinned below, including the one that matters most — an unreadable report must
protect more, not less.
"""

import json

import pytest

from backtest.artifacts import RunScratch, apply, referenced_paths, scan


def _write_report(reports_dir, name, trade_log=None, extra=None):
    payload = {"run": {"run_id": name}, **(extra or {})}
    if trade_log is not None:
        payload["trade_log_path"] = str(trade_log)
    path = reports_dir / f"orchestrator_{name}.json"
    path.write_text(json.dumps(payload))
    return path


@pytest.fixture()
def dirs(tmp_path):
    reports = tmp_path / "reports"
    cache = tmp_path / "cache"
    reports.mkdir()
    cache.mkdir()
    return reports, cache


class TestReferencedPaths:
    def test_collects_trade_log_paths_from_reports(self, dirs):
        reports, _ = dirs
        log = reports / "trade_log_orch_technical_20260812_120000_abc.csv"
        log.write_text("ticker\n")
        _write_report(reports, "job0", trade_log=log)

        pinned = referenced_paths(reports)
        assert str(log.resolve()) in pinned
        assert pinned[str(log.resolve())] == "orchestrator_job0.json"

    def test_finds_reports_in_subdirectories(self, dirs):
        """Superseded runs are quarantined into a subdirectory but are still
        real reports — their logs must stay protected while they are kept."""
        reports, _ = dirs
        sub = reports / "superseded_by_taxfix_20260812"
        sub.mkdir()
        log = reports / "trade_log_orch_technical_20260812_130000_def.csv"
        log.write_text("ticker\n")
        _write_report(sub, "job130", trade_log=log)

        assert str(log.resolve()) in referenced_paths(reports)

    def test_unreadable_report_does_not_crash_the_sweep(self, dirs):
        reports, _ = dirs
        (reports / "orchestrator_broken.json").write_text("{not json")
        assert referenced_paths(reports) == {}


class TestProtection:
    def test_referenced_trade_log_is_never_a_deletable_candidate(self, dirs):
        reports, cache = dirs
        log = reports / "trade_log_orch_technical_20260812_120000_abc.csv"
        log.write_text("x" * 100)
        _write_report(reports, "job0", trade_log=log)

        sweep = scan(reports_dir=reports, cache_dir=cache)
        assert all(c.path != log for c in sweep.deletable())

    def test_orphaned_trade_log_is_reclaimable(self, dirs):
        """No surviving report references it, so it can never be loaded or
        interpreted again."""
        reports, cache = dirs
        orphan = reports / "trade_log_orch_technical_20260101_010101_dead.csv"
        orphan.write_text("x" * 50)

        sweep = scan(reports_dir=reports, cache_dir=cache)
        assert orphan in [c.path for c in sweep.deletable()]

    def test_quarantining_a_report_releases_its_log(self, dirs):
        """The tax-fix flow: a superseded report is moved out of the tree, and
        only then may its trade log be collected."""
        reports, cache = dirs
        log = reports / "trade_log_orch_technical_20260812_120000_abc.csv"
        log.write_text("x" * 100)
        report = _write_report(reports, "job0", trade_log=log)

        assert log not in [c.path for c in scan(reports_dir=reports, cache_dir=cache).deletable()]
        report.unlink()
        assert log in [c.path for c in scan(reports_dir=reports, cache_dir=cache).deletable()]

    def test_apply_refuses_protected_candidates_even_if_asked(self, dirs):
        reports, cache = dirs
        log = reports / "trade_log_orch_technical_20260812_120000_abc.csv"
        log.write_text("x" * 100)
        _write_report(reports, "job0", trade_log=log)

        sweep = scan(reports_dir=reports, cache_dir=cache)
        apply(sweep, dry_run=False)
        assert log.exists(), "a protected artifact must survive apply()"


class TestTradeBooks:
    def test_unreferenced_trade_book_is_reclaimable(self, dirs):
        reports, cache = dirs
        book = reports / "trade_book_orch_technical_20260812_120000_abc.csv"
        book.write_text("x" * 200)
        sweep = scan(reports_dir=reports, cache_dir=cache)
        assert book in [c.path for c in sweep.deletable()]

    def test_can_be_kept_by_flag(self, dirs):
        reports, cache = dirs
        book = reports / "trade_book_orch_technical_20260812_120000_abc.csv"
        book.write_text("x" * 200)
        sweep = scan(reports_dir=reports, cache_dir=cache, include_trade_books=False)
        assert book not in [c.path for c in sweep.candidates]


class TestAgeFilter:
    def test_recent_artifacts_are_skipped(self, dirs):
        reports, cache = dirs
        (reports / "trade_book_orch_technical_20260812_120000_abc.csv").write_text("x")
        sweep = scan(reports_dir=reports, cache_dir=cache, min_age_days=1.0)
        assert sweep.candidates == []


class TestDryRun:
    def test_dry_run_reports_bytes_but_deletes_nothing(self, dirs):
        reports, cache = dirs
        book = reports / "trade_book_orch_technical_20260812_120000_abc.csv"
        book.write_text("x" * 500)

        sweep = scan(reports_dir=reports, cache_dir=cache)
        assert apply(sweep, dry_run=True) == 500
        assert book.exists()

        assert apply(sweep, dry_run=False) == 500
        assert not book.exists()


class TestRunScratch:
    def test_removes_itself_on_success(self, tmp_path):
        with RunScratch("job1", root=tmp_path) as scratch:
            (scratch / "tmp.parquet").write_text("data")
            created = scratch
        assert not created.exists()

    def test_keeps_scratch_on_error_for_diagnosis(self, tmp_path):
        with pytest.raises(ValueError):
            with RunScratch("job2", root=tmp_path) as scratch:
                (scratch / "half-written.parquet").write_text("data")
                raise ValueError("boom")
        assert (tmp_path / "scratch" / "job2").exists()

    def test_default_root_is_not_tmpfs(self):
        """/tmp is RAM-backed here (7.3 GB tmpfs); scratch must not compete with
        workers for the memory that has already caused OOM kills. This asserts
        the DEFAULT root — passing root=tmp_path would only test the fixture,
        which itself lives under /tmp."""
        scratch = RunScratch("job3")
        assert not str(scratch.path).startswith("/tmp/")
        assert "cache" in scratch.path.parts

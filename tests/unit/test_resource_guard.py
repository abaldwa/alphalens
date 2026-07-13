"""
tests/unit/test_resource_guard.py

Phase: Pipeline & Monitoring Remediation, Phase 2
Owner: Platform / Scheduler
Consumers: CI, pytest

Unit tests for ingestion/scheduler/resource_guard.py's self-healing
adaptive chunk sizing. RSS reads are monkeypatched — these tests never
depend on the actual host's real memory pressure.
"""


from ingestion.scheduler import resource_guard


class TestCurrentRssMb:
    def test_returns_a_positive_number_for_this_real_process(self):
        # No mocking here: this process is definitely using some memory.
        assert resource_guard.current_rss_mb() > 0.0


class TestMemoryPressureHigh:
    def test_low_rss_is_not_high_pressure(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "current_rss_mb", lambda: 100.0)
        assert resource_guard.memory_pressure_high(ceiling_mb=1000.0) is False

    def test_rss_above_high_water_mark_is_high_pressure(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "current_rss_mb", lambda: 900.0)
        assert resource_guard.memory_pressure_high(ceiling_mb=1000.0) is True

    def test_default_ceiling_reads_from_settings(self, monkeypatch):
        import config.settings as settings_mod

        monkeypatch.setattr(settings_mod, "PIPELINE_MEMORY_CEILING_MB", 500.0)
        monkeypatch.setattr(resource_guard, "current_rss_mb", lambda: 450.0)
        assert resource_guard.memory_pressure_high() is True


class TestAdaptiveChunkSize:
    def test_normal_conditions_returns_configured_size(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "memory_pressure_high", lambda **kwargs: False)
        assert resource_guard.adaptive_chunk_size(50) == 50

    def test_high_pressure_halves_chunk_size(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "memory_pressure_high", lambda **kwargs: True)
        assert resource_guard.adaptive_chunk_size(50) == 25

    def test_never_shrinks_below_floor(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "memory_pressure_high", lambda **kwargs: True)
        assert resource_guard.adaptive_chunk_size(8, floor=5) == 5

    def test_configured_size_already_at_or_below_floor_is_unchanged(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "memory_pressure_high", lambda **kwargs: True)
        assert resource_guard.adaptive_chunk_size(5, floor=5) == 5

    def test_repeated_high_pressure_converges_to_floor_not_below(self, monkeypatch):
        monkeypatch.setattr(resource_guard, "memory_pressure_high", lambda **kwargs: True)
        size = 50
        for _ in range(10):
            size = resource_guard.adaptive_chunk_size(size, floor=5)
        assert size == 5

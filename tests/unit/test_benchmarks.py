"""
Unit tests for benchmark coverage and selection (A97, A98).

Pure logic over IndexCoverage objects -- no DB.

The back-computation tests are the ones that matter. NSE publishes an index
before it launched by computing it retrospectively, and those rows are
indistinguishable from live ones except that Open/High/Low are null. A
2009-2026 backtest benchmarked against Nifty Microcap 250 is compared against
fourteen years of retrospective computation, and a report that presents that
as equivalent to a traded series is making a claim it cannot support.
"""

from datetime import date, timedelta

import pytest

from config.benchmarks import (
    benchmark_options,
    DEFAULT_BENCHMARK_INDEX,
    DEFAULT_REGIME_INDEX,
    RANK_BAND_BENCHMARKS,
    STALE_AFTER_DAYS,
    IndexCoverage,
    default_benchmark_for,
    usable_benchmarks,
)

TODAY = date.today()


def _cov(name="Nifty 500", *, first=date(2006, 4, 3), last=None, live_from=date(2006, 4, 3),
         backcomputed=0, rows=5000):
    return IndexCoverage(
        index_name=name,
        first_date=first,
        last_date=last if last is not None else TODAY - timedelta(days=1),
        n_rows=rows,
        live_from=live_from,
        n_backcomputed=backcomputed,
    )


class TestFreshness:
    def test_recent_index_is_fresh(self):
        assert _cov(last=TODAY - timedelta(days=2)).is_fresh

    def test_long_weekend_plus_holiday_does_not_trip_staleness(self):
        assert _cov(last=TODAY - timedelta(days=STALE_AFTER_DAYS)).is_fresh

    def test_abandoned_index_is_stale(self):
        assert not _cov(last=TODAY - timedelta(days=STALE_AFTER_DAYS + 1)).is_fresh

    def test_empty_index_is_not_fresh(self):
        assert not IndexCoverage("X", None, None, 0, None, 0).is_fresh


class TestCoverage:
    def test_publication_lag_still_counts_as_covering(self):
        """Nifty 500's last row is routinely a day or two behind the last
        trading day. Reporting 'no data covering' for that is both false and
        unhelpful."""
        cov = _cov(last=TODAY - timedelta(days=2))
        assert cov.covers(date(2009, 4, 1), TODAY)

    def test_series_starting_after_the_window_does_not_cover_it(self):
        cov = _cov(first=date(2019, 1, 14))
        assert not cov.covers(date(2009, 4, 1), TODAY)

    def test_abandoned_series_does_not_cover_a_window_ending_today(self):
        """Months of lag is a real gap, unlike days of it."""
        cov = _cov(last=date(2020, 1, 1))
        assert not cov.covers(date(2009, 4, 1), TODAY)


class TestBackComputedHistory:
    def test_window_inside_live_history_is_sound(self):
        cov = _cov("Nifty Midcap 150", first=date(2008, 1, 1),
                   live_from=date(2019, 1, 14), backcomputed=2731)
        assert cov.is_live_over(date(2020, 1, 1), TODAY)
        assert cov.comparison_caveat(date(2020, 1, 1), TODAY) is None

    def test_window_predating_launch_is_flagged(self):
        """The whole point: the rows exist, so nothing errors -- the
        comparison just quietly means something weaker."""
        cov = _cov("Nifty Midcap 150", first=date(2008, 1, 1),
                   live_from=date(2019, 1, 14), backcomputed=2731)
        assert not cov.is_live_over(date(2009, 4, 1), TODAY)
        caveat = cov.comparison_caveat(date(2009, 4, 1), TODAY)
        assert "2019-01-14" in caveat
        assert "back-computation" in caveat

    def test_index_live_throughout_has_no_caveat(self):
        assert _cov("Nifty 50").comparison_caveat(date(2009, 4, 1), TODAY) is None

    def test_uncovered_window_caveat_names_what_is_available(self):
        cov = _cov("Nifty Microcap 250", first=date(2008, 1, 1),
                   live_from=date(2022, 1, 10), backcomputed=3472)
        caveat = cov.comparison_caveat(date(2000, 1, 1), TODAY)
        assert "no data covering" in caveat

    def test_stale_index_caveat_mentions_the_pipeline(self):
        cov = _cov("Nifty Whatever", last=TODAY - timedelta(days=60))
        assert "daily pipeline" in cov.comparison_caveat(date(2009, 4, 1), TODAY)


class TestUsableBenchmarks:
    def test_stale_indices_excluded_by_default(self):
        """Only indices the daily pipeline actually keeps current are offered
        for returns comparison -- a stalled one would produce a benchmark CAGR
        over a shorter period than the strategy, invisibly."""
        coverage = {
            "Nifty 50": _cov("Nifty 50"),
            "Nifty Midcap 150": _cov("Nifty Midcap 150", last=TODAY - timedelta(days=90)),
        }
        assert usable_benchmarks(coverage) == ["Nifty 50"]

    def test_require_fresh_false_includes_them(self):
        coverage = {
            "Nifty 50": _cov("Nifty 50"),
            "Nifty Midcap 150": _cov("Nifty Midcap 150", last=TODAY - timedelta(days=90)),
        }
        assert set(usable_benchmarks(coverage, require_fresh=False)) == {
            "Nifty 50",
            "Nifty Midcap 150",
        }

    def test_index_with_no_rows_excluded(self):
        coverage = {"Nifty 50": IndexCoverage("Nifty 50", None, None, 0, None, 0)}
        assert usable_benchmarks(coverage) == []

    def test_unknown_index_not_offered(self):
        assert usable_benchmarks({"Nifty Whatever": _cov("Nifty Whatever")}) == []


class TestDefaultSelection:
    @pytest.mark.parametrize(
        "band,expected",
        [(1, "Nifty 50"), (2, "Nifty Next 50"), (3, "Nifty Midcap 100"),
         (4, "Nifty Midcap 150"), (6, "Nifty Smallcap 250"),
         (7, "Nifty Smallcap 250"), (8, "Nifty Microcap 250")],
    )
    def test_rank_band_maps_to_a_size_matched_index(self, band, expected):
        """A rank 150-200 band measured against Nifty 500 is scored partly on
        the large/small-cap spread rather than on the strategy."""
        assert default_benchmark_for(channel="momentum", rank_band=band) == expected

    def test_every_rank_band_has_a_benchmark(self):
        from features.momentum_universe import RANK_BANDS

        assert {b[0] for b in RANK_BANDS} <= set(RANK_BAND_BENCHMARKS)

    def test_universe_spec_drives_non_momentum_channels(self):
        assert default_benchmark_for(
            channel="technical", universe_spec="smallcap_top_250"
        ) == "Nifty Smallcap 250"

    def test_falls_back_to_broad_index(self):
        assert default_benchmark_for(channel="technical") == DEFAULT_BENCHMARK_INDEX

    def test_regime_index_is_a_separate_decision_from_the_benchmark(self):
        """A98: one parameter drove both, so changing a report's comparison
        also changed which regimes the strategy was allowed to trade in."""
        assert DEFAULT_REGIME_INDEX == "Nifty 500"
        assert default_benchmark_for(channel="momentum", rank_band=1) != DEFAULT_REGIME_INDEX


class TestWindowDependentOptions:
    """Which indices may serve as a benchmark depends on the window, because
    NSE back-computes a series before it launched and those rows look exactly
    like live ones apart from a null Open."""

    @staticmethod
    def _coverage():
        return {
            "Nifty 50": _cov("Nifty 50"),
            "Nifty 500": _cov("Nifty 500"),
            "Nifty Next 50": _cov("Nifty Next 50", first=date(2008, 1, 1),
                                  live_from=date(2008, 1, 1)),
            "Nifty Midcap 100": _cov("Nifty Midcap 100", first=date(2008, 1, 1),
                                     live_from=date(2008, 1, 1)),
            "Nifty Midcap 150": _cov("Nifty Midcap 150", first=date(2008, 1, 1),
                                     live_from=date(2019, 1, 14), backcomputed=2731),
            "Nifty Smallcap 250": _cov("Nifty Smallcap 250", first=date(2008, 1, 1),
                                       live_from=date(2019, 1, 14), backcomputed=2731),
            "Nifty Microcap 250": _cov("Nifty Microcap 250", first=date(2008, 1, 1),
                                       live_from=date(2022, 1, 10), backcomputed=3472),
        }

    def test_recent_window_admits_the_2019_indices(self):
        opts = benchmark_options(self._coverage(), date(2020, 1, 1), TODAY)
        assert "Nifty Midcap 150" in opts["live"]
        assert "Nifty Smallcap 250" in opts["live"]

    def test_long_window_excludes_them(self):
        """Over 2009-2026 those indices did not trade for the first decade."""
        opts = benchmark_options(self._coverage(), date(2009, 4, 1), TODAY)
        assert "Nifty Midcap 150" not in opts["live"]
        assert "Nifty Midcap 150" in opts["backcomputed"]
        assert "Nifty Microcap 250" in opts["backcomputed"]

    def test_indices_live_throughout_qualify_on_any_window(self):
        for start in (date(2009, 4, 1), date(2020, 1, 1)):
            live = benchmark_options(self._coverage(), start, TODAY)["live"]
            assert {"Nifty 50", "Nifty Midcap 100", "Nifty Next 50"} <= set(live)

    def test_preferred_index_used_when_it_traded(self):
        opts = benchmark_options(
            self._coverage(), date(2020, 1, 1), TODAY, preferred="Nifty Midcap 150"
        )
        assert opts["recommended"] == "Nifty Midcap 150"
        assert opts["preferred_available"] is True

    def test_falls_back_to_nifty_500_when_the_index_did_not_exist(self):
        """A104: fall back to the broad index rather than ranking on a series
        that was retrospectively computed."""
        opts = benchmark_options(
            self._coverage(), date(2009, 4, 1), TODAY, preferred="Nifty Midcap 150"
        )
        assert opts["preferred_available"] is False
        assert opts["recommended"] == "Nifty 500"

    def test_fallback_states_its_reason_and_its_own_limitation(self):
        """The substitution has to be visible: Nifty 500 is broad, so part of
        any excess return against it is the size spread, not the strategy."""
        opts = benchmark_options(
            self._coverage(), date(2009, 4, 1), TODAY, preferred="Nifty Midcap 150"
        )
        reason = opts["fallback_reason"]
        assert "Nifty Midcap 150" in reason
        assert "2019-01-14" in reason
        assert "Nifty 500" in reason
        assert "size" in reason

    def test_no_fallback_reason_when_the_preferred_index_is_used(self):
        opts = benchmark_options(
            self._coverage(), date(2020, 1, 1), TODAY, preferred="Nifty Midcap 150"
        )
        assert opts["fallback_reason"] is None

    def test_no_fallback_reason_when_no_preference_was_expressed(self):
        opts = benchmark_options(self._coverage(), date(2009, 4, 1), TODAY)
        assert opts["fallback_reason"] is None

    def test_broad_fallback_when_no_size_index_traded(self):
        coverage = {
            "Nifty 500": _cov("Nifty 500"),
            "Nifty Smallcap 250": _cov("Nifty Smallcap 250", first=date(2008, 1, 1),
                                       live_from=date(2019, 1, 14), backcomputed=2731),
        }
        opts = benchmark_options(
            coverage, date(2009, 4, 1), TODAY, preferred="Nifty Smallcap 250"
        )
        assert opts["recommended"] == "Nifty 500"
        assert "Nifty Smallcap 250" in opts["fallback_reason"]

    def test_backcomputed_options_are_offered_not_hidden(self):
        """The user may still choose one deliberately; it just carries a
        caveat rather than disappearing."""
        opts = benchmark_options(self._coverage(), date(2009, 4, 1), TODAY)
        assert opts["backcomputed"]
        assert set(opts["backcomputed"]).isdisjoint(opts["live"])

    def test_window_shorter_than_history_still_excludes_uncovered_indices(self):
        coverage = {"Nifty 50": _cov("Nifty 50", first=date(2015, 1, 1),
                                     live_from=date(2015, 1, 1))}
        opts = benchmark_options(coverage, date(2009, 4, 1), TODAY)
        assert opts["live"] == []

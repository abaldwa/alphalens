"""
tests/unit/test_momentum_universe_bands.py

RANK_BANDS must PARTITION the rank space: contiguous, no gaps, no overlaps.

Written 2026-08-18 when the user confirmed the 200/300/500 boundaries were
unintended. Before that, bands 6/7/8 started on the rank the band below
ended on, so ranks 200, 300 and 500 each belonged to two supposedly distinct
band universes at once — a stock could be in two "separate" backtests, and
the two results were not the independent samples they looked like.

The earlier 150 overlap (bands 3/4) had already been fixed the same way, and
the fact that it recurred at three more boundaries is the reason this is now
a test rather than a comment asking someone to check.
"""

from features.momentum_universe import RANK_BANDS


def test_bands_are_contiguous_with_no_gap_or_overlap():
    ordered = sorted(RANK_BANDS, key=lambda b: b[1])
    previous_end = 0
    for band_id, rank_start, rank_end in ordered:
        assert rank_start == previous_end + 1, (
            f"band {band_id} starts at rank {rank_start} but the previous band "
            f"ended at {previous_end}. A start equal to the previous end puts one "
            f"stock in two band universes; a larger gap silently drops it from all."
        )
        assert rank_end >= rank_start, f"band {band_id} ends before it starts"
        previous_end = rank_end


def test_band_ids_are_unique():
    ids = [b[0] for b in RANK_BANDS]
    assert len(ids) == len(set(ids)), f"duplicate band ids in RANK_BANDS: {ids}"


def test_bands_cover_the_declared_range():
    """1-800 is the range the momentum sweep is defined over; a change to
    either end should be a deliberate edit to this assertion, not a silent
    drift in the constant."""
    ordered = sorted(RANK_BANDS, key=lambda b: b[1])
    assert ordered[0][1] == 1
    assert ordered[-1][2] == 800

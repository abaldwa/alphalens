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


def test_band_ids_are_contiguous_from_one():
    """2026-08-19, user-specified: the ids are 1..7 with no gap.

    Before this, the ids ran 1,2,3,4,6,7,8 — id 5 was a hole left by the
    retired 100-200 band, and every report that numbered its rows 1-7 was
    therefore off by one against the band ids from band 5 onward.
    """
    ids = sorted(b[0] for b in RANK_BANDS)
    assert ids == list(range(1, len(RANK_BANDS) + 1)), (
        f"band ids must be contiguous from 1, got {ids}"
    )


def test_legacy_band_id_map_preserves_ranges():
    """The 2026-08-19 renumber was a pure relabel: an old id and the current
    id it maps to must describe the SAME rank range, or historical results
    read through the map would be attributed to a universe they never ran
    against."""
    from features.momentum_universe import LEGACY_BAND_ID_TO_CURRENT, RETIRED_BAND_IDS

    by_id = {b[0]: (b[1], b[2]) for b in RANK_BANDS}
    legacy_ranges = {6: (201, 300), 7: (301, 500), 8: (501, 800)}
    for old_id, new_id in LEGACY_BAND_ID_TO_CURRENT.items():
        assert by_id[new_id] == legacy_ranges[old_id], (
            f"old band {old_id} was {legacy_ranges[old_id]} but current band "
            f"{new_id} is {by_id[new_id]} — this is not a relabel"
        )
    # A retired id must NOT be claimed as a relabel of a live one.
    assert not (set(RETIRED_BAND_IDS) & set(LEGACY_BAND_ID_TO_CURRENT))

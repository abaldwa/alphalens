"""
tests/unit/test_momentum_universe_bands.py

Each band FAMILY must partition the rank space: contiguous, no gaps, no
overlaps WITHIN the family.

Written 2026-08-18 when the user confirmed the 200/300/500 boundaries were
unintended. Before that, bands 6/7/8 started on the rank the band below
ended on, so ranks 200, 300 and 500 each belonged to two supposedly distinct
band universes at once — a stock could be in two "separate" backtests, and
the two results were not the independent samples they looked like.

The earlier 150 overlap (bands 3/4) had already been fixed the same way, and
the fact that it recurred at three more boundaries is the reason this is now
a test rather than a comment asking someone to check.

2026-08-20: RANK_BANDS now holds TWO families — the original seven ranges and
five new ones (1-75, 76-160, 161-275, 276-550, 551-800) — interleaved and
renumbered together by (rank_start, rank_end). Across families the ranges
overlap ON PURPOSE: M1 (1-50) and M2 (1-75) are alternative slicings run side
by side for comparison, not a single cover. So the no-overlap property is
asserted per family, which is where it still has to hold; asserting it over
the flat list would now fail for the right reasons and would have to be
deleted, losing the check that actually matters.
"""

import pytest

from features.momentum_universe import LEGACY_BAND_IDS, RANK_BANDS, V2_BAND_IDS


@pytest.mark.parametrize("family_name,family_ids", [("legacy", LEGACY_BAND_IDS), ("v2", V2_BAND_IDS)])
def test_bands_are_contiguous_with_no_gap_or_overlap(family_name, family_ids):
    bands = [b for b in RANK_BANDS if b[0] in family_ids]
    assert bands, f"family {family_name} matched no bands in RANK_BANDS"
    ordered = sorted(bands, key=lambda b: b[1])
    previous_end = 0
    for band_id, rank_start, rank_end in ordered:
        assert rank_start == previous_end + 1, (
            f"[{family_name}] band {band_id} starts at rank {rank_start} but the "
            f"previous band ended at {previous_end}. A start equal to the previous "
            f"end puts one stock in two band universes; a larger gap silently drops "
            f"it from all."
        )
        assert rank_end >= rank_start, f"band {band_id} ends before it starts"
        previous_end = rank_end


def test_every_band_belongs_to_exactly_one_family():
    """A band missing from both families would silently escape the contiguity
    check above — the one property this file exists to guarantee."""
    ids = {b[0] for b in RANK_BANDS}
    assigned = set(LEGACY_BAND_IDS) | set(V2_BAND_IDS)
    assert ids == assigned, (
        f"unassigned band ids {ids - assigned}, unknown ids {assigned - ids}"
    )
    assert not (set(LEGACY_BAND_IDS) & set(V2_BAND_IDS)), "a band is in both families"


def test_band_ids_are_ordered_by_rank_range():
    """2026-08-20, user-specified: ids ascend by (rank_start, rank_end) across
    BOTH families, so the number tells you where the band sits on the ladder
    regardless of which family added it."""
    ordered = sorted(RANK_BANDS, key=lambda b: (b[1], b[2]))
    assert ordered == sorted(RANK_BANDS, key=lambda b: b[0]), (
        "band ids do not ascend with (rank_start, rank_end): "
        f"{[(b[0], b[1], b[2]) for b in sorted(RANK_BANDS, key=lambda b: b[0])]}"
    )


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
    """Every renumber has been a pure relabel: an old id and the current id it
    maps to must describe the SAME rank range, or historical results read
    through the map would be attributed to a universe they never ran against.

    Keys are the ORIGINAL pre-2026-08-19 ids, so this composes both renumbers
    (2026-08-19's 6/7/8 -> 5/6/7 and 2026-08-20's twelve-band expansion) in a
    single hop.
    """
    from features.momentum_universe import LEGACY_BAND_ID_TO_CURRENT, RETIRED_BAND_IDS

    by_id = {b[0]: (b[1], b[2]) for b in RANK_BANDS}
    original_ranges = {
        1: (1, 50), 2: (51, 100), 3: (101, 150), 4: (151, 200),
        6: (201, 300), 7: (301, 500), 8: (501, 800),
    }
    assert set(LEGACY_BAND_ID_TO_CURRENT) == set(original_ranges), (
        "the map must cover every original band id and no others"
    )
    for old_id, new_id in LEGACY_BAND_ID_TO_CURRENT.items():
        assert by_id[new_id] == original_ranges[old_id], (
            f"old band {old_id} was {original_ranges[old_id]} but current band "
            f"{new_id} is {by_id[new_id]} — this is not a relabel"
        )
    # A retired id must NOT be claimed as a relabel of a live one.
    assert not (set(RETIRED_BAND_IDS) & set(LEGACY_BAND_ID_TO_CURRENT))
